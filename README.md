# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"  
uvicorn app.main:app --port 8000
```
Indexes are created automatically at startup. MongoDB 6.0+ is required (`$densify`, `$setWindowFields`).

## Status
All endpoints in `openapi.yaml` are implemented, including `/admin/explain/{endpoint}`.
Pipelines for the monthly summary, department summary, late leaderboard and department trend run entirely in MongoDB.

## Notes
* Analytics read the stored derived values (`work_hours`, `late_minutes`, ...); they are never recomputed.
* Reported numbers are rounded half-up using exact decimal arithmetic.
* Concurrency: unique indexes (employee, punch-in), compare-and-set updates (punch-out, regularize; the loser gets 409).
