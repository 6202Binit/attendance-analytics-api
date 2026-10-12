"""
Run from the repository root against a REAL MongoDB 6.0+:

    pip install httpx
    MONGO_URI=mongodb://localhost:27017 python verify_with_mongo.py

Uses its own database (attendance_verify), drops it first, seeds awkward cases directly into
MongoDB (like the grader does) and checks every analytics endpoint against hand-calculated values.
July 2026: the 1st is a Wednesday; weekends are 4,5,11,12,18,19,25,26.
"""
import json, os
os.environ["MONGO_DB"] = "attendance_verify"
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from app import main as m

m.client.drop_database("attendance_verify")
UTC = timezone.utc
def dt(d, h=4, mi=0): y, mo, dd = map(int, d.split("-")); return datetime(y, mo, dd, h, mi, tzinfo=UTC)

def emp(code, dept, joined, ss="09:30", se="18:30"):
    return dict(emp_code=code, name=f"Name {code}", email=f"{code}@x.com", department=dept,
                shift_start=ss, shift_end=se, joined_on=joined, created_at=dt(joined))
def log(code, d, status="PRESENT", wh=None, late=0, ot=0, half=False):
    has_in = status in ("PRESENT", "WFH", "ON_DUTY")
    return dict(emp_code=code, date=d, status=status, punch_in=dt(d) if has_in else None,
                punch_out=dt(d, 12) if has_in and wh is not None else None, work_hours=wh,
                late_minutes=late, overtime_minutes=ot, half_day=half, history=[])

m.db.employees.insert_many([
    emp("EMP0001", "Eng", "2026-01-05"), emp("EMP0002", "Eng", "2026-07-13"),
    emp("EMP0003", "Sales", "2026-02-01"), emp("EMP0004", "Sales", "2026-02-01"),
    emp("EMP0006", "Support", "2026-03-10"), emp("EMP0007", "Support", "2026-08-01"),
    emp("EMP0008", "Support", "2026-03-10"),   # no logs at all
])
m.db.attendance_logs.insert_many([
    log("EMP0001", "2026-07-06", wh=9.0, late=35), log("EMP0001", "2026-07-07", wh=3.5, late=12, half=True),
    log("EMP0001", "2026-07-08", "LEAVE"), log("EMP0001", "2026-07-09", "WFH", wh=8.0),
    log("EMP0001", "2026-07-10", "ON_DUTY", wh=7.5), log("EMP0001", "2026-07-11", wh=8.0, late=20),  # Saturday
    log("EMP0002", "2026-07-13", wh=9.0), log("EMP0002", "2026-07-14", wh=8.0),
    log("EMP0003", "2026-07-06", wh=8.0, late=30), log("EMP0003", "2026-07-07", wh=8.0, late=30),
    log("EMP0003", "2026-07-12", wh=6.0, late=5, ot=40),                                             # Sunday
    log("EMP0004", "2026-07-06", wh=8.0, late=35), log("EMP0004", "2026-07-07", wh=8.0, late=30),
    log("EMP0006", "2026-07-08", wh=4.4, late=20, half=True),
    log("EMP9999", "2026-07-06", wh=8.0, late=500),                                                  # unknown employee
])
m.db.attendance_logs.insert_one({k: v for k, v in log("EMP0002", "2026-07-15", wh=8.0).items() if k not in ("half_day", "history")})  # legacy doc

with TestClient(m.app) as c:   # `with` runs startup => creates indexes
    def get(url): r = c.get(url); assert r.status_code == 200, (url, r.status_code, r.text); return r.json()
    def check(name, got, want): assert got == want, f"{name}\n  got : {got}\n  want: {want}"

    # --- employee monthly
    j = get("/analytics/employees/EMP0001/monthly?month=2026-07")
    check("monthly EMP0001", j, dict(emp_code="EMP0001", month="2026-07", working_days=23, present_days=3.5, leave_days=1,
          late_count=3, total_late_minutes=67, total_overtime_minutes=0, attendance_pct=15.22))
    j = get("/analytics/employees/EMP0002/monthly?month=2026-07")
    check("monthly EMP0002 (mid-month joiner)", (j["working_days"], j["present_days"], j["attendance_pct"]), (15, 3.0, 20.0))
    j = get("/analytics/employees/EMP0008/monthly?month=2026-07")
    check("monthly no logs", (j["present_days"], j["attendance_pct"]), (0.0, 0.0))
    j = get("/analytics/employees/EMP0007/monthly?month=2026-07")
    check("monthly joins later", (j["working_days"], j["attendance_pct"]), (0, None))
    assert c.get("/analytics/employees/NOPE/monthly?month=2026-07").status_code == 404
    assert c.get("/analytics/employees/EMP0001/monthly?month=2026-13").status_code == 422

    # --- department summary
    items = {i["department"]: i for i in get("/analytics/departments/summary?month=2026-07")["items"]}
    check("departments sorted", list(items), ["Eng", "Sales", "Support"])
    check("Eng", items["Eng"], dict(department="Eng", headcount=2, present_days=6.5, avg_work_hours=7.63, late_count=3,
          total_late_minutes=67, leave_count=1, on_duty_count=1))
    check("Sales", items["Sales"], dict(department="Sales", headcount=2, present_days=4.0, avg_work_hours=7.6, late_count=5,
          total_late_minutes=130, leave_count=0, on_duty_count=0))
    check("Support (EMP0007 not joined, EMP0008 has no logs)", items["Support"], dict(department="Support", headcount=2,
          present_days=0.5, avg_work_hours=4.4, late_count=1, total_late_minutes=20, leave_count=0, on_duty_count=0))
    june = {i["department"]: i for i in get("/analytics/departments/summary?month=2026-06")["items"]}
    check("June Eng", (june["Eng"]["headcount"], june["Eng"]["present_days"], june["Eng"]["avg_work_hours"]), (1, 0.0, None))
    check("filter", [i["department"] for i in get("/analytics/departments/summary?month=2026-07&department=Sales")["items"]], ["Sales"])

    # --- leaderboard (EMP0001 67; EMP0003 65 = EMP0004 65; EMP0006 20; EMP9999 ignored)
    rows = lambda q: [(r["rank"], r["emp_code"], r["total_late_minutes"], r["late_count"]) for r in get(f"/analytics/leaderboard/late?month=2026-07{q}")["items"]]
    check("limit=2 returns the tie too", rows("&limit=2"), [(1, "EMP0001", 67, 3), (2, "EMP0003", 65, 3), (2, "EMP0004", 65, 2)])
    check("limit=3 (rank 4 not included)", len(rows("&limit=3")), 3)
    check("limit=4", rows("&limit=4")[-1], (4, "EMP0006", 20, 1))
    check("department ranking", rows("&department=Sales"), [(1, "EMP0003", 65, 3), (1, "EMP0004", 65, 2)])

    # --- trend, Eng
    t = get("/analytics/departments/Eng/trend?from=2026-07-06&to=2026-07-14")["items"]
    check("trend rows", len(t), 9)
    check("trend rates", [r["attendance_rate"] for r in t], [1.0, 0.5, 0.0, 1.0, 1.0, None, None, 0.5, 0.5])
    check("trend moving avg", [r["moving_avg_7d"] for r in t], [1.0, 0.75, 0.5, 0.625, 0.7, 0.7, 0.7, 0.6, 0.6])
    check("trend headcount", [r["headcount"] for r in t], [1, 1, 1, 1, 1, 1, 1, 2, 2])
    check("trend present", [r["present_count"] for r in t], [1, 0.5, 0, 1, 1, 1, 0, 1, 1])
    check("trend late", [r["late_count"] for r in t], [1, 1, 0, 0, 0, 1, 0, 0, 0])
    check("trend working-day flags", [r["is_working_day"] for r in t], [True] * 5 + [False] * 2 + [True] * 2)
    empty = get("/analytics/departments/Eng/trend?from=2026-09-01&to=2026-09-07")["items"]   # no records at all
    check("empty range still has 7 rows", len(empty), 7)
    check("empty range rates", [r["attendance_rate"] for r in empty], [0.0] * 4 + [None, None, 0.0])
    assert c.get("/analytics/departments/Nope/trend?from=2026-07-01&to=2026-07-02").status_code == 404
    assert c.get("/analytics/departments/Eng/trend?from=2026-07-02&to=2026-07-01").status_code == 422
    assert c.get("/analytics/departments/Eng/trend?from=2026-01-01&to=2026-06-01").status_code == 422   # > 92 days

    # --- explain: every endpoint must use an index and never a collection scan
    urls = {
        "attendance_list": "?date_from=2026-07-01&date_to=2026-07-31", "employee_monthly": "?emp_code=EMP0001&month=2026-07",
        "department_summary": "?month=2026-07", "late_leaderboard": "?month=2026-07&limit=5",
        "department_trend": "?department=Eng&from=2026-07-01&to=2026-07-31",
    }
    for ep, q in urls.items():
        j = get(f"/admin/explain/{ep}{q}"); text = json.dumps(j["explain"])
        assert "IXSCAN" in text and "COLLSCAN" not in text, f"{ep}: IXSCAN={'IXSCAN' in text} COLLSCAN={'COLLSCAN' in text}"
        print("explain ok:", ep, "->", j["collection"])
    assert c.get("/admin/explain/employee_monthly").status_code == 422
print("ALL CHECKS PASSED")
