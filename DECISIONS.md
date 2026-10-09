# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (no duplicate employees, every lookup by code), `department+emp_code` (department filter sorted by code, trend), `joined_on` (summary headcount). `attendance_logs`: unique `emp_code+date` (one record per day, monthly range, every `$lookup`), `date desc+emp_code` (list sort, leaderboard month scan), `status+date+emp_code` (status filter, same sort). I rejected a separate `emp_code` index because the compound unique index already starts with it.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one`. The unique index lets MongoDB accept only one; the other gets `DuplicateKeyError`, which I return as 409. No check-then-insert.

3. **Ties.** `$setWindowFields` with `$rank` gives 1, 2, 2, 4. I filter `rank <= limit` after ranking, so everyone tied at the cutoff is returned, even if that is more than `limit` rows, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`), then `$lookup`s their logs. An employee with no logs still is one document, so `headcount` counts them.

5. **100x data.** Pre-aggregate daily per-department and per-employee counters when records are written, so analytics read small summary collections.
