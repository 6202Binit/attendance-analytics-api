# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees; unique (`emp_code`, `date`) on logs, which also serves per-employee date ranges and the monthly summary; (`date` desc, `emp_code`) for the default list order and for month-wide scans; (`emp_code`, `punch_in` desc) for finding the open record at punch-out; (`status`, `date`, `emp_code`) for status filters; (`department`, `joined_on`) for headcount. I rejected an index on `late_minutes`: almost every record has 0, so it filters badly.

2. **Punch-in race.** Both requests compute the same `date` and try `insert_one`. The unique (`emp_code`, `date`) index lets the database accept one and raise `DuplicateKeyError` for the other, which I turn into 409. There is no read-then-write step to race on.

3. **Ties.** I rank with `$rank` over total late minutes, then keep every row with rank <= `limit`. Tied employees share a rank, so both stay and the list can be longer than `limit`. Rows are sorted by minutes, then `emp_code`.

4. **Headcount.** The summary starts from `employees` (joined on or before month end) and looks logs up per employee. An employee with no logs gets an empty array and counts as 0 days, but still counts in headcount.

5. **100x data.** I would stop computing monthly numbers from raw logs on every request and keep pre-aggregated monthly totals, updated on punch and correction.
