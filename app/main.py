"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded for convenience; real environment variables win)

Layout of this file
    1. configuration / database / indexes
    2. time helpers and business rules R1-R5, R8
    3. request models and serializers
    4. employees, attendance (punch-in / punch-out / regularize / list)
    5. analytics (aggregation pipelines)
    6. admin explain
"""
import calendar
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import date as date_type
from datetime import datetime, time as time_type, timedelta, timezone
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration, database, indexes
# --------------------------------------------------------------------------- #
load_dotenv()  # override=False by default: real environment variables take priority
log = logging.getLogger("attendance")

# tz_aware=True: PyMongo hands back timezone-aware (UTC) datetimes instead of naive ones,
# so we never mix naive and aware values in arithmetic.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")  # R6
ABSENCE_STATUSES = ("ABSENT", "LEAVE")
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

LATE_GRACE_SECONDS = 10 * 60  # R2
OVERTIME_MIN_MINUTES = 30  # R3
HALF_DAY_BELOW_HOURS = Decimal("4.50")  # R5
MAX_SHIFT_SECONDS = 24 * 3600  # punch-out must be within 24h of punch-in

# Index names are explicit so queries can hint them and the explain endpoint is predictable.
IDX_EMP_UNIQUE = "uniq_emp_code"
IDX_EMP_DEPT_CODE = "dept_emp_code"
IDX_EMP_DEPT_JOINED = "dept_joined_on"
IDX_EMP_JOINED = "joined_on"
IDX_LOG_UNIQUE = "uniq_emp_date"
IDX_LOG_DATE = "date_desc_emp"
IDX_LOG_PUNCH = "emp_punch_in"
IDX_LOG_STATUS = "status_date_emp"


def ensure_indexes() -> None:
    """Idempotent: creating an index that already exists with the same spec is a no-op."""
    db.employees.create_indexes(
        [
            # Uniqueness of emp_code is enforced by the database, not by check-then-insert.
            IndexModel([("emp_code", ASCENDING)], name=IDX_EMP_UNIQUE, unique=True),
            # GET /employees?department=X sorted by emp_code.
            IndexModel([("department", ASCENDING), ("emp_code", ASCENDING)], name=IDX_EMP_DEPT_CODE),
            # Headcount (R9): department + joined_on <= end of period.
            IndexModel([("department", ASCENDING), ("joined_on", ASCENDING)], name=IDX_EMP_DEPT_JOINED),
            IndexModel([("joined_on", ASCENDING)], name=IDX_EMP_JOINED),
        ]
    )
    db.attendance_logs.create_indexes(
        [
            # One record per (emp_code, date): the guard against duplicate punch-ins, and the
            # access path for "one employee, a date range" (list filter, monthly summary).
            IndexModel([("emp_code", ASCENDING), ("date", ASCENDING)], name=IDX_LOG_UNIQUE, unique=True),
            # Default list order (date desc, emp_code asc) and every "all employees in a month" scan
            # (department summary, leaderboard, trend).
            IndexModel([("date", DESCENDING), ("emp_code", ASCENDING)], name=IDX_LOG_DATE),
            # Punch-out: "latest record of this employee with punch_in <= t".
            IndexModel([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name=IDX_LOG_PUNCH),
            # List filtered by status, still sorted by date desc, emp_code asc.
            IndexModel(
                [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name=IDX_LOG_STATUS
            ),
        ]
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    last_error: Optional[Exception] = None
    for _attempt in range(10):  # MongoDB may still be starting; the app must be ready within 20 s
        try:
            ensure_indexes()
            last_error = None
            break
        except PyMongoError as exc:
            last_error = exc
            log.warning("index creation failed (%s), retrying", exc)
            time.sleep(1)
    if last_error is not None:
        raise last_error
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    log.exception("database error", exc_info=exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def unprocessable(msg: str, loc: tuple = ("body",)) -> None:
    """Raise a 422 shaped like FastAPI's own validation errors."""
    raise HTTPException(status_code=422, detail=[{"loc": list(loc), "msg": msg, "type": "value_error"}])


# --------------------------------------------------------------------------- #
# 2. Time helpers and business rules
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    """Current time as an aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.now(UTC).replace(microsecond=0)


def from_millis(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated (floored) to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Make sure a datetime read from MongoDB is aware (treat naive values as UTC)."""
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def to_millis(value: Optional[datetime]) -> Optional[int]:
    """Datetime -> integer epoch milliseconds, using integer arithmetic only (no float rounding)."""
    value = as_utc(value)
    if value is None:
        return None
    return calendar.timegm(value.utctimetuple()) * 1000 + value.microsecond // 1000


def round_half_up(value: Any, places: int = 2) -> float:
    """R8: round half-up (not banker's rounding, not binary-float rounding)."""
    quantum = Decimal(1).scaleb(-places)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


def _hm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded "HH:MM" strings compare correctly


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date_type:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in
    belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < time_type(*_hm(shift_end)):
        day -= timedelta(days=1)
    return day


def shift_start_at(day: date_type, shift_start: str) -> datetime:
    return datetime.combine(day, time_type(*_hm(shift_start)), tzinfo=IST)


def shift_end_at(day: date_type, shift_start: str, shift_end: str) -> datetime:
    end_day = day + timedelta(days=1) if is_overnight(shift_start, shift_end) else day
    return datetime.combine(end_day, time_type(*_hm(shift_end)), tzinfo=IST)


def compute_late_minutes(punch_in: datetime, day: date_type, shift_start: str) -> int:
    """R2: late only when strictly more than 10:00 after shift_start; then whole minutes since shift_start."""
    seconds = int((punch_in - shift_start_at(day, shift_start)).total_seconds())
    return seconds // 60 if seconds > LATE_GRACE_SECONDS else 0


def compute_overtime(punch_out: datetime, day: date_type, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes after shift_end, counted only when >= 30."""
    seconds = int((punch_out - shift_end_at(day, shift_start, shift_end)).total_seconds())
    minutes = seconds // 60
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up. Decimal keeps exact ties exact."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_derived(
    shift_start: str, shift_end: str, day: date_type, punch_in: datetime, punch_out: Optional[datetime]
) -> dict:
    """All stored derived values for a presence record (DATA_MODEL.md, section 3)."""
    derived = {
        "late_minutes": compute_late_minutes(punch_in, day, shift_start),
        "work_hours": None,
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        work_hours = compute_work_hours(punch_in, punch_out)
        derived["work_hours"] = work_hours
        derived["overtime_minutes"] = compute_overtime(punch_out, day, shift_start, shift_end)
        derived["half_day"] = Decimal(str(work_hours)) < HALF_DAY_BELOW_HOURS  # rounded value (R5)
    return derived


DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def strict_date(value: str) -> Optional[date_type]:
    """Parse exactly YYYY-MM-DD (date.fromisoformat alone also accepts other ISO shapes)."""
    if not isinstance(value, str) or not DATE_RE.match(value):
        return None
    try:
        return date_type.fromisoformat(value)
    except ValueError:
        return None


def parse_date_param(value: str, loc: tuple) -> date_type:
    parsed = strict_date(value)
    if parsed is None:
        unprocessable("must be a valid date in YYYY-MM-DD format", loc)
    return parsed


def month_bounds(month: str) -> tuple[date_type, date_type]:
    try:
        year, mon = int(month[:4]), int(month[5:7])
        first = date_type(year, mon, 1)
    except (ValueError, IndexError):
        unprocessable("must be a month in YYYY-MM format", ("query", "month"))
    last = date_type(year, mon, calendar.monthrange(year, mon)[1])
    return first, last


def count_working_days(start: date_type, end: date_type) -> int:
    """R7: Monday-Friday only, no holiday calendar."""
    count, day = 0, start
    while day <= end:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


# --------------------------------------------------------------------------- #
# 3. Request models and serializers
# --------------------------------------------------------------------------- #
EPOCH_MS_MIN = 100_000_000_000  # below this a number looks like seconds
EPOCH_MS_MAX = 4_102_444_800_000
# strict=True: floats, strings and booleans are rejected instead of silently coerced.
EpochMs = Annotated[int, Field(strict=True, ge=EPOCH_MS_MIN, le=EPOCH_MS_MAX)]
TIME_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=TIME_PATTERN)
    shift_end: str = Field(default="18:30", pattern=TIME_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_joined_on(cls, value: str) -> str:
        if strict_date(value) is None:
            raise ValueError("must be a valid date in YYYY-MM-DD format")
        return value

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # Omitted means "unchanged"; an explicit null is not a valid value for these fields.
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} must not be null")
        return self


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": to_millis(doc.get("created_at")),
    }


def serialize_history_entry(entry: dict) -> dict:
    changes = {}
    for field, change in (entry.get("changes") or {}).items():
        if field in ("punch_in", "punch_out"):  # nested BSON dates -> epoch ms as well
            changes[field] = {"from": to_millis(change.get("from")), "to": to_millis(change.get("to"))}
        else:
            changes[field] = {"from": change.get("from"), "to": change.get("to")}
    return {"at": to_millis(entry["at"]), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def serialize_record(doc: dict) -> dict:
    """Public shape of an attendance record. Missing legacy fields get their documented defaults;
    _id never leaves the service."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_millis(doc.get("punch_in")),
        "punch_out": to_millis(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [serialize_history_entry(h) for h in (doc.get("history") or [])],
    }


def shifts_of(emp: dict) -> tuple[str, str]:
    return emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")


# --------------------------------------------------------------------------- #
# 4. Employees and attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    created = now_utc()
    doc = body.model_dump()
    doc["created_at"] = created
    try:
        db.employees.insert_one(doc)  # the unique index decides concurrent duplicates
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict = {}
    if department is not None:
        q["department"] = department
    total = db.employees.count_documents(q)  # counts the filtered set (R10)
    cursor = db.employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_employee(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    shift_start, shift_end = shifts_of(emp)
    ts = from_millis(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, shift_start, shift_end)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, day, shift_start),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent request wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    shift_start, shift_end = shifts_of(emp)
    t = from_millis(body.punched_at) if body.punched_at is not None else now_utc()

    # Most recent record whose punch_in is at or before t. No date arithmetic: this works across
    # midnight for overnight shifts. Records without punch_in (ABSENT/LEAVE) never match $lte.
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": t}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        if db.attendance_logs.find_one({"emp_code": body.emp_code, "punch_in": {"$gt": t}}) is not None:
            unprocessable("punched_at must be after punch_in", ("body", "punched_at"))
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")

    pin = as_utc(rec["punch_in"])
    if t <= pin:
        unprocessable("punched_at must be after punch_in", ("body", "punched_at"))
    if (t - pin).total_seconds() > MAX_SHIFT_SECONDS:
        unprocessable("punched_at is more than 24 hours after punch_in", ("body", "punched_at"))

    day = date_type.fromisoformat(rec["date"])
    derived = compute_derived(shift_start, shift_end, day, pin, t)
    # Compare-and-set: only succeeds while the record is still open and unchanged. Of several
    # simultaneous punch-outs exactly one matches; the rest get None and a 409.
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": rec["emp_code"], "date": rec["date"], "punch_in": rec["punch_in"], "punch_out": None},
        {"$set": {"punch_out": t, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return serialize_record(updated)


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = parse_date_param(date, ("path", "date"))
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")
    shift_start, shift_end = shifts_of(emp)

    old_status = rec["status"]
    old_in, old_out = as_utc(rec.get("punch_in")), as_utc(rec.get("punch_out"))
    new_status = body.status or old_status

    if new_status in ABSENCE_STATUSES:
        if body.punch_in is not None or body.punch_out is not None:
            unprocessable("ABSENT and LEAVE records cannot have punch times")
        new_in = new_out = None
        derived = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    else:
        new_in = from_millis(body.punch_in) if body.punch_in is not None else old_in
        if new_in is None:
            unprocessable("a presence status requires a punch_in")
        new_out = from_millis(body.punch_out) if body.punch_out is not None else old_out
        if body.punch_in is not None and attendance_date(new_in, shift_start, shift_end) != day:
            unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        if new_out is not None:
            if new_out <= new_in:
                unprocessable("punch_out must be after punch_in", ("body", "punch_out"))
            if (new_out - new_in).total_seconds() > MAX_SHIFT_SECONDS:
                unprocessable("punch_out must be within 24 hours of punch_in", ("body", "punch_out"))
        derived = compute_derived(shift_start, shift_end, day, new_in, new_out)

    old_values = {
        "status": old_status,
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    new_values = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derived}
    changes = {
        field: {"from": old_values[field], "to": new_values[field]}
        for field in old_values
        if old_values[field] != new_values[field]
    }
    # "Changes nothing" is judged on what the client controls; a stale stored derived value alone
    # must not turn a no-op request into a write.
    if not any(field in changes for field in ("status", "punch_in", "punch_out")):
        unprocessable("the request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency. Every correction appends exactly one history entry, so the history
    # length acts as a version number. If anyone modified the record after we read it, nothing
    # matches and the loser gets 409 instead of silently overwriting history.
    history = rec.get("history")
    guard = {
        "emp_code": emp_code,
        "date": day.isoformat(),
        "status": old_status,
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "history": {"$size": len(history)} if isinstance(history, list) else None,
    }
    update: dict = {"$set": dict(new_values)}
    if isinstance(history, list):
        update["$push"] = {"history": entry}
    else:  # legacy record without a history array
        update["$set"]["history"] = [entry]
    updated = db.attendance_logs.find_one_and_update(guard, update, return_document=ReturnDocument.AFTER)
    if updated is None:
        raise HTTPException(409, "record was modified concurrently, retry")
    return serialize_record(updated)


def build_attendance_query(
    emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str], status: Optional[str],
    page: int, page_size: int,
) -> tuple[dict, list, int, int]:
    """Shared by GET /attendance and its explain, so the explained query is the real one."""
    d_from = parse_date_param(date_from, ("query", "date_from")) if date_from is not None else None
    d_to = parse_date_param(date_to, ("query", "date_to")) if date_to is not None else None
    if d_from and d_to and d_from > d_to:
        unprocessable("date_from must not be after date_to", ("query", "date_from"))
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if d_from or d_to:
        q["date"] = {}
        if d_from:
            q["date"]["$gte"] = d_from.isoformat()
        if d_to:
            q["date"]["$lte"] = d_to.isoformat()
    if status is not None:
        q["status"] = status
    sort = [("date", DESCENDING), ("emp_code", ASCENDING)]
    return q, sort, (page - 1) * page_size, page_size


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q, sort, skip, limit = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(q)  # count with the same filters (R10)
    docs = db.attendance_logs.find(q).sort(sort).skip(skip).limit(limit)  # the database sorts and pages
    return {"items": [serialize_record(d) for d in docs], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5. Analytics (aggregation pipelines)
# --------------------------------------------------------------------------- #
# Shared expression fragments. Stored derived values are authoritative (DATA_MODEL.md), so they
# are read as stored; $ifNull supplies the documented defaults for legacy records.
LATE = {"$ifNull": ["$late_minutes", 0]}
OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
IS_PRESENT = {"$in": ["$status", list(PRESENCE_STATUSES)]}
# $dayOfWeek: 1 = Sunday ... 7 = Saturday, so Monday-Friday is 2..6. The date string is parsed
# as UTC midnight, which is the same weekday as the calendar date.
WEEKDAY_OF_DATE = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None}}}
IS_WEEKDAY = {"$in": [WEEKDAY_OF_DATE, [2, 3, 4, 5, 6]]}
# A present record is worth 1 day, a half day 0.5 (R5).
PRESENT_UNITS = {"$cond": [IS_PRESENT, {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}, 0]}
IS_LATE = {"$cond": [{"$gt": [LATE, 0]}, 1, 0]}


def run_aggregate(collection: str, pipeline: list, hint: Optional[str] = None) -> list:
    options = {"hint": hint} if hint else {}
    return list(db[collection].aggregate(pipeline, **options))


def month_range_strings(month: str) -> tuple[str, str]:
    first, last = month_bounds(month)
    return first.isoformat(), last.isoformat()


# ---- employee monthly --------------------------------------------------------
def build_monthly_pipeline(emp_code: str, month: str) -> tuple[list, str]:
    start, end = month_range_strings(month)
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": {"$cond": [IS_WEEKDAY, PRESENT_UNITS, 0]}},  # Mon-Fri only (R7)
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": IS_LATE},
                "total_late_minutes": {"$sum": LATE},  # weekend records count here
                "total_overtime_minutes": {"$sum": OVERTIME},
            }
        },
    ]
    return pipeline, IDX_LOG_UNIQUE


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_PATTERN)):
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    first, last = month_bounds(month)
    joined = strict_date(emp.get("joined_on", ""))
    working_days = count_working_days(max(first, joined) if joined else first, last)  # R7

    pipeline, hint = build_monthly_pipeline(emp_code, month)
    rows = run_aggregate("attendance_logs", pipeline, hint)
    row = rows[0] if rows else {}
    present_days = round_half_up(row.get("present_days", 0), 2)
    pct = None
    if working_days > 0:
        pct = round_half_up(Decimal(str(present_days)) * 100 / Decimal(working_days), 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


# ---- department summary --------------------------------------------------------
def build_summary_pipeline(month: str, department: Optional[str]) -> tuple[list, str]:
    """Runs on `employees`, so every employee that has joined by month end is counted (R9) whether
    or not they have logs. Logs are joined in per employee through the (emp_code, date) index."""
    start, end = month_range_strings(month)
    match: dict = {"joined_on": {"$lte": end}}
    if department is not None:
        match["department"] = department
    has_hours = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}
    pipeline = [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start, "$lte": end}}},
                    {
                        "$group": {
                            "_id": None,
                            "present_days": {"$sum": {"$cond": [IS_WEEKDAY, PRESENT_UNITS, 0]}},
                            "late_count": {"$sum": IS_LATE},
                            "total_late_minutes": {"$sum": LATE},
                            "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                            # Hours are summed as whole hundredths (integers) and the mean is taken
                            # over records, not over per-employee means.
                            "work_cents": {
                                "$sum": {"$cond": [has_hours, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}
                            },
                            "work_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
                        }
                    },
                ],
                "as": "m",
            }
        },
    ]

    def first_metric(name: str) -> dict:
        # An employee with no logs has an empty `m`; $ifNull turns that into 0.
        return {"$sum": {"$ifNull": [{"$arrayElemAt": [f"$m.{name}", 0]}, 0]}}

    pipeline += [
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": first_metric("present_days"),
                "late_count": first_metric("late_count"),
                "total_late_minutes": first_metric("total_late_minutes"),
                "leave_count": first_metric("leave_count"),
                "on_duty_count": first_metric("on_duty_count"),
                "work_cents": first_metric("work_cents"),
                "work_n": first_metric("work_n"),
            }
        },
        {"$sort": {"_id": 1}},
    ]
    return pipeline, (IDX_EMP_DEPT_JOINED if department is not None else IDX_EMP_JOINED)


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_PATTERN), department: Optional[str] = None):
    pipeline, hint = build_summary_pipeline(month, department)
    items = []
    for row in run_aggregate("employees", pipeline, hint):
        work_n = int(row["work_n"])
        avg = None
        if work_n:
            avg = round_half_up(Decimal(str(row["work_cents"])) / Decimal(work_n) / 100, 2)
        items.append(
            {
                "department": row["_id"],
                "headcount": int(row["headcount"]),
                "present_days": round_half_up(row["present_days"], 2),
                "avg_work_hours": avg,
                "late_count": int(row["late_count"]),
                "total_late_minutes": int(row["total_late_minutes"]),
                "leave_count": int(row["leave_count"]),
                "on_duty_count": int(row["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


# ---- late leaderboard ---------------------------------------------------------
def build_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> tuple[list, str]:
    start, end = month_range_strings(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees have no match and are dropped
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank only inside the department
    pipeline += [
        # $rank = standard competition ranking: ties share a rank and the next rank is skipped.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies to the rank, not to the row count
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline, IDX_LOG_DATE


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    pipeline, hint = build_leaderboard_pipeline(month, limit, department)
    rows = run_aggregate("attendance_logs", pipeline, hint)
    return {
        "month": month,
        "items": [
            {
                "rank": int(r["rank"]),
                "emp_code": r["emp_code"],
                "name": r["name"],
                "department": r["department"],
                "total_late_minutes": int(r["total_late_minutes"]),
                "late_count": int(r["late_count"]),
            }
            for r in rows
        ],
    }


# ---- department trend ---------------------------------------------------------
MAX_TREND_DAYS = 92


def load_trend_inputs(department: str, date_from: str, date_to: str) -> tuple[list, date_type, date_type]:
    d_from = parse_date_param(date_from, ("query", "from"))
    d_to = parse_date_param(date_to, ("query", "to"))
    if d_to < d_from:
        unprocessable("'to' must not be before 'from'", ("query", "to"))
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        unprocessable(f"the range must not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    employees = list(db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not employees:
        raise HTTPException(404, "department not found")
    return employees, d_from, d_to


def build_trend_pipeline(employees: list, d_from: date_type, d_to: date_type) -> tuple[list, str]:
    codes = [e["emp_code"] for e in employees]
    joined = sorted(e["joined_on"] for e in employees)  # headcount (R9) is a count over this list
    days = (d_to - d_from).days + 1
    lower = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    upper = lower + timedelta(days=days)  # $densify's upper bound is exclusive
    dec = Decimal128
    pipeline = [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
        {"$group": {"_id": "$date", "present": {"$sum": PRESENT_UNITS}, "late": {"$sum": IS_LATE}}},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # Gap filling: one document for every calendar day in [from, to], weekends included.
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        {
            "$addFields": {
                "present": {"$ifNull": ["$present", 0]},
                "late": {"$ifNull": ["$late", 0]},
                "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
                "dow": {"$dayOfWeek": "$d"},
            }
        },
        {"$match": {"date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
        {
            "$addFields": {
                "is_working_day": {"$in": ["$dow", [2, 3, 4, 5, 6]]},
                "headcount": {"$size": {"$filter": {"input": joined, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}},
            }
        },
        {
            # Rate in ten-thousandths, rounded half-up with exact decimal arithmetic.
            "$addFields": {
                "rate_k": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$toInt": {
                                "$floor": {
                                    "$add": [
                                        {
                                            "$multiply": [
                                                {"$divide": [{"$toDecimal": "$present"}, {"$toDecimal": "$headcount"}]},
                                                dec("10000"),
                                            ]
                                        },
                                        dec("0.5"),
                                    ]
                                }
                            }
                        },
                        None,
                    ]
                }
            }
        },
        # Mean of the non-null rates in this row and up to 6 rows before it, never before `from`.
        {"$setWindowFields": {"sortBy": {"d": 1}, "output": {"mov_k": {"$avg": "$rate_k", "window": {"documents": [-6, 0]}}}}},
        {"$sort": {"d": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present": 1, "late": 1, "rate_k": 1, "mov_k": 1}},
    ]
    return pipeline, IDX_LOG_DATE


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, date_from: str = Query(..., alias="from"), date_to: str = Query(..., alias="to")):
    employees, d_from, d_to = load_trend_inputs(department, date_from, date_to)
    pipeline, hint = build_trend_pipeline(employees, d_from, d_to)
    items = []
    for row in run_aggregate("attendance_logs", pipeline, hint):
        rate_k, mov_k = row.get("rate_k"), row.get("mov_k")
        moving = None
        if mov_k is not None:  # 4 decimals half-up of a mean expressed in ten-thousandths
            moving = int((Decimal(str(mov_k)) + Decimal("0.5")).to_integral_value(ROUND_FLOOR)) / 10000
        items.append(
            {
                "date": row["date"],
                "is_working_day": bool(row["is_working_day"]),
                "headcount": int(row["headcount"]),
                "present_count": float(row["present"]),
                "late_count": int(row["late"]),
                "attendance_rate": None if rate_k is None else rate_k / 10000,
                "moving_avg_7d": moving,
            }
        )
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 6. Admin: explain
# --------------------------------------------------------------------------- #
def run_explain(command: dict) -> dict:
    result = db.command({"explain": command, "verbosity": "executionStats"})
    # Round-trip through bson's relaxed extended JSON so ObjectId/dates/Int64 become plain JSON.
    return json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))


def aggregate_command(collection: str, pipeline: list, hint: str) -> dict:
    return {"aggregate": collection, "pipeline": pipeline, "cursor": {}, "hint": hint}


def require(value: Any, name: str) -> Any:
    if value is None:
        unprocessable(f"{name} is required for this endpoint", ("query", name))
    return value


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    """Explains the main query of the named endpoint, built by the same builder function the endpoint uses."""
    if endpoint == "attendance_list":
        q, sort, skip, lim = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        collection = "attendance_logs"
        command = {"find": collection, "filter": q, "sort": dict(sort), "skip": skip, "limit": lim}
    elif endpoint == "employee_monthly":
        pipeline, hint = build_monthly_pipeline(require(emp_code, "emp_code"), require(month, "month"))
        collection = "attendance_logs"
        command = aggregate_command(collection, pipeline, hint)
    elif endpoint == "department_summary":
        pipeline, hint = build_summary_pipeline(require(month, "month"), department)
        collection = "employees"
        command = aggregate_command(collection, pipeline, hint)
    elif endpoint == "late_leaderboard":
        pipeline, hint = build_leaderboard_pipeline(require(month, "month"), limit, department)
        collection = "attendance_logs"
        command = aggregate_command(collection, pipeline, hint)
    else:  # department_trend
        employees, d_from, d_to = load_trend_inputs(
            require(department, "department"), require(from_, "from"), require(to, "to")
        )
        pipeline, hint = build_trend_pipeline(employees, d_from, d_to)
        collection = "attendance_logs"
        command = aggregate_command(collection, pipeline, hint)
    return {"endpoint": endpoint, "collection": collection, "explain": run_explain(command)}
