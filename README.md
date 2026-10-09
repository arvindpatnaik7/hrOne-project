# Employee Attendance & Analytics API

FastAPI + MongoDB service for the HROne engineering assignment. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # or export MONGO_URI / MONGO_DB
python sample_seed.py                                  # optional: loads the 9 sample documents
uvicorn app.main:app --port 8000
```
Needs MongoDB 6.0+ (`$densify`, `$setWindowFields`). `MONGO_URI` and `MONGO_DB` come from the environment; a `.env`
file only fills in values that are not already set. Interactive docs: http://localhost:8000/docs

## Notes
- **Indexes** are created at startup (idempotent). If MongoDB is not reachable at startup, the app still starts,
  `/health` returns 503, and the indexes are created on the first successful health check.
- **Time.** The client uses `tz_aware=True`; instants are epoch ms in the API and BSON dates in MongoDB, truncated to
  whole seconds before any calculation (R1). Calendar logic (attendance date, shift start/end) is done in IST.
- **Rounding.** Half-up everywhere: `Decimal` in Python, and `floor(x * 10^n + 0.5) / 10^n` on Decimal128 inside
  pipelines, because MongoDB's `$round` rounds half to even.
- **Concurrency.** Punch-in and employee creation rely on unique indexes; punch-out is a conditional update
  (`punch_out: null`); regularization uses optimistic concurrency on the history length, so a concurrent editor
  gets 409 instead of losing an entry.
- **Analytics** are single aggregation pipelines. Department summary and trend start from `employees` so that
  employees without logs are counted; the trend fills missing days with `$densify` and computes headcount and the
  7-day moving average with `$setWindowFields`.
- `GET /admin/explain/{endpoint}` runs `explain` (executionStats) on the same query/pipeline builder the endpoint uses.
- Choices where the contract left room: the trend accepts at most 92 days counted inclusively (a full quarter);
  explicit `null` for `status` / `punch_in` / `punch_out` in a PATCH is a 422; a record's analytics use its stored
  derived values as they are.

See `REVIEW.md` for the defects fixed in the starter and `DECISIONS.md` for design choices.
