"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file (everything lives here on purpose, see README.md):
  1. Configuration and database client
  2. Time helpers          - epoch ms <-> BSON datetime, IST calendar, R1 attendance date
  3. Business-rule helpers - R2 late, R3 overtime, R4 work hours, R5 half day, R7 working days
  4. Request models and validation
  5. Serialisation         - MongoDB document -> API JSON
  6. Query / pipeline builders (shared by the endpoints AND by /admin/explain)
  7. Endpoints
"""
import calendar
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration and database client
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # .env only fills in what the real environment does not set

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True: PyMongo returns timezone-aware UTC datetimes instead of naive ones, so every
# datetime we touch is unambiguous. A short server-selection timeout keeps /health honest.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
OVERTIME_MIN_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"

# Indexes: (collection, keys, options). Created idempotently at startup; see DECISIONS.md.
INDEXES = [
    # Uniqueness of the business key, and the target of every employee lookup.
    (employees, [("emp_code", ASCENDING)], {"unique": True, "name": "uniq_emp_code"}),
    # GET /employees?department=  (filter + sort by emp_code) and the trend's department match.
    (employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "department_emp_code"}),
    # Department summary without a department filter: joined_on <= end of month (R9).
    (employees, [("joined_on", ASCENDING)], {"name": "joined_on"}),
    # One record per (emp_code, date): makes punch-in race-safe. Also serves the employee-monthly
    # range scan, every $lookup from employees into logs, punch-out and the emp_code filter of the list.
    (logs, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "uniq_emp_code_date"}),
    # GET /attendance sort (date desc, emp_code asc) and every month/day range scan (leaderboard).
    (logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_desc_emp_code"}),
    # GET /attendance?status=  -> equality, then the same sort (ESR rule).
    (logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp_code"}),
]


_indexes_ready = False


def ensure_indexes() -> bool:
    """Idempotent: create_index is a no-op when the same index already exists."""
    global _indexes_ready
    for collection, keys, options in INDEXES:
        try:
            collection.create_index(keys, **options)
        except ConnectionFailure as exc:  # MongoDB not reachable yet: stop here, /health retries later
            print(f"WARNING: MongoDB unreachable, indexes not created yet: {exc}")
            return False
        except PyMongoError as exc:  # e.g. existing data violates a unique index: report, keep going
            print(f"WARNING: could not create index {options['name']}: {exc}")
    _indexes_ready = True
    return True


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(ConnectionFailure)
async def db_unavailable(_: Request, exc: ConnectionFailure):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def unprocessable(msg: str, loc: tuple = ("body",)) -> HTTPException:
    """A 422 whose body has the same shape as FastAPI's own validation errors."""
    return HTTPException(422, detail=[{"loc": list(loc), "msg": msg, "type": "value_error"}])


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: BSON datetimes are UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def is_overnight(emp: dict) -> bool:
    return emp["shift_end"] <= emp["shift_start"]  # zero-padded HH:MM compares correctly as text


def ist_at(day: date, hhmm: str) -> datetime:
    h, m = parse_hhmm(hhmm)
    return datetime(day.year, day.month, day.day, h, m, tzinfo=IST)


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: the IST calendar date of the punch-in, except that on an overnight shift a punch-in
    earlier than shift_end belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    if is_overnight(emp) and (local.hour, local.minute, local.second) < (*parse_hhmm(emp["shift_end"]), 0):
        return local.date() - timedelta(days=1)
    return local.date()


def shift_bounds(day: date, emp: dict) -> tuple[datetime, datetime]:
    """Start and end instants of the shift that belongs to attendance date `day`."""
    start = ist_at(day, emp["shift_start"])
    end = ist_at(day, emp["shift_end"])
    if is_overnight(emp):
        end += timedelta(days=1)
    return start, end


def parse_date(value: str, loc: tuple) -> date:
    if not isinstance(value, str) or not DATE_RE.match(value):
        raise unprocessable("must be a date in YYYY-MM-DD format", loc)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise unprocessable("not a valid calendar date", loc)


def month_range(month: str) -> tuple[date, date]:
    y, m = int(month[:4]), int(month[5:7])
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


# --------------------------------------------------------------------------- #
# 3. Business-rule helpers (pure functions, unit-testable)
# --------------------------------------------------------------------------- #
def compute_late_minutes(punch_in: datetime, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; minutes counted from shift_start."""
    start, _ = shift_bounds(attendance_date(punch_in, emp), emp)
    seconds = int((punch_in - start).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up (Decimal, not float round())."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: whole minutes after shift_end, only when >= 30. Overnight shifts end on the next day."""
    _, end = shift_bounds(day, emp)
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def compute_half_day(work_hours: float) -> bool:
    """R5: on the *rounded* work hours."""
    return Decimal(str(work_hours)) < HALF_DAY_HOURS


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: date, emp: dict) -> dict:
    """All four derived fields for a record, per the table in DATA_MODEL.md."""
    punch_in = punch_in.replace(microsecond=0) if punch_in else None  # R1: whole seconds, also for
    punch_out = punch_out.replace(microsecond=0) if punch_out else None  # values already stored
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, emp)
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    wh = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": wh,
        "late_minutes": late,
        "overtime_minutes": compute_overtime(punch_out, day, emp),
        "half_day": compute_half_day(wh),
    }


def working_days(first: date, last: date) -> int:
    """R7: Mon-Fri between first and last inclusive (no holiday calendar)."""
    if first > last:
        return 0
    total = (last - first).days + 1
    full_weeks, rest = divmod(total, 7)
    count = full_weeks * 5
    for i in range(rest):
        if (first + timedelta(days=full_weeks * 7 + i)).weekday() < 5:
            count += 1
    return count


# --------------------------------------------------------------------------- #
# 4. Request models and validation
# --------------------------------------------------------------------------- #
# Epoch milliseconds: a real JSON integer (no floats, strings or booleans) in the allowed range.
# Anything below 1e11 looks like seconds and is rejected.
EpochMs = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
HHMM = Annotated[str, StringConstraints(pattern=HHMM_PATTERN)]


class EmployeeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: Annotated[str, StringConstraints(pattern=r"^EMP\d{4,6}$")]
    name: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    email: Annotated[str, StringConstraints(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, StringConstraints(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]

    @model_validator(mode="after")
    def check(self):
        try:
            date.fromisoformat(self.joined_on)
        except ValueError:
            raise ValueError("joined_on is not a valid calendar date")
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")  # derived fields sent by a client are ignored

    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, StringConstraints(min_length=5, max_length=200)]
    regularized_by: Annotated[str, StringConstraints(min_length=1, max_length=50)]

    @model_validator(mode="after")
    def no_explicit_nulls(self):
        for field in ("status", "punch_in", "punch_out"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} can not be null")
        return self


PageQ = Annotated[int, Query(ge=1)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]
MonthQ = Annotated[str, Query(pattern=MONTH_PATTERN)]
LimitQ = Annotated[int, Query(ge=1, le=50)]


# --------------------------------------------------------------------------- #
# 5. Serialisation
# --------------------------------------------------------------------------- #
TIME_FIELDS = ("punch_in", "punch_out")


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def history_out(entry: dict) -> dict:
    changes = {}
    for field, change in (entry.get("changes") or {}).items():
        if field in TIME_FIELDS:
            changes[field] = {"from": dt_to_ms(change.get("from")), "to": dt_to_ms(change.get("to"))}
        else:
            changes[field] = {"from": change.get("from"), "to": change.get("to")}
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Seeded records may omit history / half_day / late / overtime: missing means [] / false / 0."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 6. Query / pipeline builders
#    Each builder is used by its endpoint AND by /admin/explain, so the explained plan is
#    exactly the plan the endpoint runs.
# --------------------------------------------------------------------------- #
D_HALF = Decimal128("0.5")
D_ZERO = Decimal128("0")


def agg_half_up(expr: Any, places: int) -> dict:
    """Half-up rounding inside the pipeline. $round is banker's rounding (2.345 -> 2.34), so we do
    floor(x * 10^p + 0.5) / 10^p on Decimal128 values, which are exact for these numbers."""
    factor = 10 ** places
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, factor]}, D_HALF]}}, factor]}


def agg_weekday_of(date_field: str) -> dict:
    """ISO day of week (1 = Mon ... 7 = Sun) of a 'YYYY-MM-DD' string field."""
    return {"$isoDayOfWeek": {"$dateFromString": {"dateString": date_field, "format": "%Y-%m-%d", "timezone": "UTC"}}}


IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
IS_HALF = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
PRESENT_VALUE = {"$cond": [IS_PRESENT, {"$cond": [IS_HALF, 0.5, 1]}, 0]}  # R5 + R6, any day
PRESENT_WEEKDAY_VALUE = {"$cond": [{"$lte": [agg_weekday_of("$date"), 5]}, PRESENT_VALUE, 0]}  # + R7
LATE_MIN = {"$ifNull": ["$late_minutes", 0]}
IS_LATE = {"$gt": [LATE_MIN, 0]}


def build_attendance_query(emp_code, date_from, date_to, status, page, page_size) -> dict:
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status:
        q["status"] = status
    return {
        "filter": q,
        "sort": [("date", DESCENDING), ("emp_code", ASCENDING)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
    }


def build_employee_monthly(emp_code: str, month: str, wd: int) -> list:
    first, last = month_range(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": PRESENT_WEEKDAY_VALUE},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [IS_LATE, 1, 0]}},
            "total_late_minutes": {"$sum": LATE_MIN},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
        {"$project": {
            "_id": 0,
            "present_days": 1, "leave_days": 1, "late_count": 1,
            "total_late_minutes": 1, "total_overtime_minutes": 1,
            "attendance_pct": {"$cond": [
                {"$gt": [wd, 0]},
                {"$toDouble": agg_half_up({"$divide": [{"$multiply": [{"$toDecimal": "$present_days"}, 100]}, wd]}, 2)},
                None,
            ]},
        }},
    ]


def build_department_summary(month: str, department: Optional[str]) -> list:
    """Starts from employees so that people with zero logs are still counted (R9)."""
    first, last = month_range(month)
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    has_hours = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                {"$group": {
                    "_id": None,
                    "present_days": {"$sum": PRESENT_WEEKDAY_VALUE},
                    "wh_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$work_hours"}, D_ZERO]}},
                    "wh_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
                    "late_count": {"$sum": {"$cond": [IS_LATE, 1, 0]}},
                    "total_late_minutes": {"$sum": {"$cond": [IS_LATE, LATE_MIN, 0]}},
                    "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                    "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                }},
            ],
            "as": "stats",
        }},
        # an employee with no logs has stats == [] -> $first gives a missing field, which $sum ignores
        {"$project": {"department": 1, "s": {"$first": "$stats"}}},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$s.present_days"},
            "wh_sum": {"$sum": "$s.wh_sum"},
            "wh_n": {"$sum": "$s.wh_n"},
            "late_count": {"$sum": "$s.late_count"},
            "total_late_minutes": {"$sum": "$s.total_late_minutes"},
            "leave_count": {"$sum": "$s.leave_count"},
            "on_duty_count": {"$sum": "$s.on_duty_count"},
        }},
        {"$sort": {"_id": 1}},
        {"$project": {
            "_id": 0,
            "department": "$_id",
            "headcount": 1,
            "present_days": {"$toDouble": "$present_days"},
            # mean over records (sum / count), not a mean of per-employee means
            "avg_work_hours": {"$cond": [
                {"$gt": ["$wh_n", 0]},
                {"$toDouble": agg_half_up({"$divide": ["$wh_sum", "$wh_n"]}, 2)},
                None,
            ]},
            "late_count": 1, "total_late_minutes": 1, "leave_count": 1, "on_duty_count": 1,
        }},
    ]


def build_late_leaderboard(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_range(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}],
            "as": "emp",
        }},
        {"$unwind": "$emp"},  # drops logs whose employee does not exist
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank is standard competition ranking: ties share a rank and the next rank is skipped (1,2,2,4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applied AFTER ranking, so ties at the cutoff are kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ]
    return pipeline


def build_department_trend(department: str, first: date, last: date) -> list:
    """One row per calendar day, filled by $densify; headcount and moving average by $setWindowFields."""
    f, t = first.isoformat(), last.isoformat()
    lower = datetime(first.year, first.month, first.day, tzinfo=UTC)
    upper = datetime(last.year, last.month, last.day, tzinfo=UTC) + timedelta(days=1)  # densify upper bound is exclusive
    rate_ok = {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": f, "$lte": t}}},
                {"$project": {"_id": 0, "date": 1, "p": PRESENT_VALUE, "l": {"$cond": [IS_LATE, 1, 0]}}},
            ],
            "as": "logs",
        }},
        # Turn every employee into "events" on days:
        #   - a sentinel on day `from` (guarantees at least one row for $densify),
        #   - +1 headcount on the day they start counting (joined_on, or `from` if they joined earlier),
        #   - one event per log with its presence (0 / 0.5 / 1) and late flag.
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            [{"date": f, "hc": 0, "p": 0, "l": 0}],
            {"$cond": [{"$lte": ["$joined_on", t]}, [{"date": {"$max": ["$joined_on", f]}, "hc": 1, "p": 0, "l": 0}], []]},
            {"$map": {"input": "$logs", "as": "x", "in": {"date": "$$x.date", "hc": 0, "p": "$$x.p", "l": "$$x.l"}}},
        ]}}},
        {"$unwind": "$events"},
        {"$group": {"_id": "$events.date", "hc": {"$sum": "$events.hc"}, "p": {"$sum": "$events.p"}, "l": {"$sum": "$events.l"}}},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}}}},
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        # running total of joiners = headcount on that day (R9)
        {"$setWindowFields": {"sortBy": {"d": 1}, "output": {
            "headcount": {"$sum": {"$ifNull": ["$hc", 0]}, "window": {"documents": ["unbounded", "current"]}},
        }}},
        {"$addFields": {
            "present_count": {"$ifNull": ["$p", 0]},
            "late_count": {"$ifNull": ["$l", 0]},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$d"}, 5]},
        }},
        {"$addFields": {"rate": {"$cond": [
            rate_ok,
            agg_half_up({"$divide": [{"$toDecimal": "$present_count"}, {"$toDecimal": "$headcount"}]}, 4),
            None,
        ]}}},
        # mean of the non-null rates in the current row and up to 6 preceding rows ($avg skips nulls)
        {"$setWindowFields": {"sortBy": {"d": 1}, "output": {
            "ma": {"$avg": "$rate", "window": {"documents": [-6, 0]}},
        }}},
        {"$sort": {"d": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d", "timezone": "UTC"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": {"$toDouble": "$present_count"},
            "late_count": 1,
            "attendance_rate": {"$cond": [{"$eq": ["$rate", None]}, None, {"$toDouble": "$rate"}]},
            "moving_avg_7d": {"$cond": [{"$eq": ["$ma", None]}, None, {"$toDouble": agg_half_up("$ma", 4)}]},
        }},
    ]


# --------------------------------------------------------------------------- #
# 7. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:  # startup could not reach MongoDB: finish the job now
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


# ----- employees ------------------------------------------------------------ #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageQ = 1, page_size: PageSizeQ = 20):
    q = {"department": department} if department is not None else {}
    total = employees.count_documents(q) if q else employees.estimated_document_count()
    cursor = employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# ----- attendance ----------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): of two simultaneous inserts exactly one wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},  # null punch_in (ABSENT/LEAVE) never matches
        sort=[("date", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    if ts <= rec["punch_in"] or ts - rec["punch_in"] > MAX_SHIFT:
        raise unprocessable("punched_at must be after punch_in and within 24 hours", ("body", "punched_at"))

    day = date.fromisoformat(rec["date"])
    update = {"punch_out": ts, **derived_fields(rec["status"], rec["punch_in"], ts, day, emp)}
    # Conditional update: only the request that still sees punch_out == null wins.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": update},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    if date_from is not None:
        parse_date(date_from, ("query", "date_from"))
    if date_to is not None:
        parse_date(date_to, ("query", "date_to"))
    if date_from and date_to and date_from > date_to:
        raise unprocessable("date_from must not be after date_to", ("query", "date_from"))
    spec = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = logs.count_documents(spec["filter"]) if spec["filter"] else logs.estimated_document_count()
    cursor = logs.find(spec["filter"]).sort(spec["sort"]).skip(spec["skip"]).limit(spec["limit"])
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):  # noqa: A002 - `date` is the contract's name;
    day = parse_date(date, ("path", "date"))                     # the datetime.date class is not used below
    emp = get_employee_or_404(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if rec is None:
        raise HTTPException(404, "no attendance record for this date")

    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": int(rec.get("late_minutes") or 0),
        "overtime_minutes": int(rec.get("overtime_minutes") or 0),
        "half_day": bool(rec.get("half_day") or False),
    }
    new_status = body.status or old["status"]

    if new_status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"punch times can not be set with status {new_status}")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else old["punch_in"]
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else old["punch_out"]
        if new_in is None:
            raise unprocessable(f"status {new_status} requires a punch_in")
        if body.punch_in is not None and attendance_date(new_in, emp) != day:
            raise unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        if new_out is not None and (new_out <= new_in or new_out - new_in > MAX_SHIFT):
            raise unprocessable("punch_out must be after punch_in and within 24 hours", ("body", "punch_out"))

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(new_status, new_in, new_out, day, emp)}

    def comparable(field, value):
        return dt_to_ms(value) if field in TIME_FIELDS else value

    changes = {f: {"from": old[f], "to": new[f]} for f in new if comparable(f, old[f]) != comparable(f, new[f])}
    if not changes:
        raise unprocessable("the request does not change anything")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    # Optimistic concurrency: the update only applies if nobody appended to history since we read it.
    # A concurrent editor therefore gets 409 instead of silently overwriting (and losing) our entry.
    seen = len(rec.get("history") or [])
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, seen]}},
        {"$set": new, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently, retry")
    return record_out(updated)


# ----- analytics ------------------------------------------------------------ #
def monthly_working_days(emp: dict, month: str) -> int:
    first, last = month_range(month)
    joined = date.fromisoformat(emp["joined_on"])
    return working_days(max(first, joined), last)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthQ):
    emp = get_employee_or_404(emp_code)
    wd = monthly_working_days(emp, month)
    rows = list(logs.aggregate(build_employee_monthly(emp_code, month, wd)))
    s = rows[0] if rows else {}
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wd,
        "present_days": float(s.get("present_days", 0)),
        "leave_days": s.get("leave_days", 0),
        "late_count": s.get("late_count", 0),
        "total_late_minutes": s.get("total_late_minutes", 0),
        "total_overtime_minutes": s.get("total_overtime_minutes", 0),
        "attendance_pct": s.get("attendance_pct", 0.0 if wd > 0 else None),
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthQ, department: Optional[str] = None):
    items = list(employees.aggregate(build_department_summary(month, department)))
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthQ, limit: LimitQ = 10, department: Optional[str] = None):
    items = list(logs.aggregate(build_late_leaderboard(month, limit, department)))
    return {"month": month, "items": items}


def trend_range(from_: str, to: str) -> tuple[date, date]:
    first = parse_date(from_, ("query", "from"))
    last = parse_date(to, ("query", "to"))
    if last < first:
        raise unprocessable("to must not be before from", ("query", "to"))
    if (last - first).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"the range can not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    return first, last


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, Query(alias="from")],
    to: Annotated[str, Query()],
):
    first, last = trend_range(from_, to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "department not found")
    items = list(employees.aggregate(build_department_trend(department, first, last)))
    return {"department": department, "items": items}


# ----- admin ---------------------------------------------------------------- #
def to_json(doc: Any) -> Any:
    """Raw MongoDB output (Timestamps, Decimal128, datetimes...) -> plain JSON."""
    return json.loads(json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS))


def require(params: dict, *names: str) -> None:
    missing = [n for n in names if params.get(n) in (None, "")]
    if missing:
        raise HTTPException(422, detail=[
            {"loc": ["query", n], "msg": "Field required", "type": "missing"} for n in missing
        ])


ExplainName = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Annotated[ExplainName, Path()],
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: LimitQ = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    params = {"emp_code": emp_code, "month": month, "department": department, "from": from_, "to": to}

    if endpoint == "attendance_list":
        for name, value in (("date_from", date_from), ("date_to", date_to)):
            if value is not None:
                parse_date(value, ("query", name))
        if date_from and date_to and date_from > date_to:
            raise unprocessable("date_from must not be after date_to", ("query", "date_from"))
        spec = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        command = {"find": logs.name, "filter": spec["filter"], "sort": dict(spec["sort"]),
                   "skip": spec["skip"], "limit": spec["limit"]}
        collection = logs.name
    else:
        if endpoint == "employee_monthly":
            require(params, "emp_code", "month")
            emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
            wd = monthly_working_days(emp, month) if emp else 0
            pipeline, collection = build_employee_monthly(emp_code, month, wd), logs.name
        elif endpoint == "department_summary":
            require(params, "month")
            pipeline, collection = build_department_summary(month, department), employees.name
        elif endpoint == "late_leaderboard":
            require(params, "month")
            pipeline, collection = build_late_leaderboard(month, limit, department), logs.name
        else:  # department_trend
            require(params, "department", "from", "to")
            first, last = trend_range(from_, to)
            pipeline, collection = build_department_trend(department, first, last), employees.name
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    result = db.command({"explain": command, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": collection, "explain": to_json(result)}
