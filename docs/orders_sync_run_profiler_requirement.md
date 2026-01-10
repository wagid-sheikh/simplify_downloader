# **REQUIREMENT**

**Implement an AUTO backfill + continuous refresh `orders_sync_run_profiler` that decides what to run based on data in `orders_sync_log` (not manual flags), supports date-windowed pipelines, late-arrival overlap, and parallel execution across stores. Also add `orders_sync_log` and date-window support to `td_orders_sync` and `uc_orders_sync`.**

---

# **GOAL**

When `orders_sync_run_profiler` starts, it must:

1. Inspect `orders_sync_log` to determine, per **store + pipeline**, which date windows are missing or need refresh
2. Run only the required windows in **fixed N-day chunks (default 90)** until caught up to `current_date`
3. Automatically **re-run the most recent successful day(s)** to capture late-arriving data
4. Then perform a **current-day refresh**
5. Execute jobs **in parallel across stores** with safety guarantees

- No manual backfill modes.
- `orders_sync_log` is the source of truth.
- Pipelines are idempotent.

---

# **A) DDL — `orders_sync_log`**

Create table:

```sql
CREATE TABLE orders_sync_log (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL REFERENCES pipelines(id),
    run_id VARCHAR(64) NOT NULL REFERENCES pipeline_run_summaries(run_id),
    run_env VARCHAR(32) NOT NULL,
    cost_center VARCHAR(8),
    store_code VARCHAR(8) NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    orders_pulled_at TIMESTAMPTZ NULL,
    sales_pulled_at TIMESTAMPTZ NULL,
    status VARCHAR(16) NOT NULL,      -- running | partial | success | failed
    attempt_no INT NOT NULL DEFAULT 1,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Constraints and indexes:

```sql
CREATE UNIQUE INDEX uq_orders_sync_log_window
ON orders_sync_log (pipeline_id, store_code, from_date, to_date, run_id);

CREATE INDEX ix_orders_sync_log_lookup
ON orders_sync_log (pipeline_id, store_code, from_date, to_date, status);

CREATE INDEX ix_orders_sync_log_store_recent
ON orders_sync_log (store_code, created_at DESC);

CREATE INDEX ix_orders_sync_log_pipeline_recent
ON orders_sync_log (pipeline_id, created_at DESC);
```

`updated_at` must be maintained on every update.

`pipeline_id` must reference the existing `pipelines` table row for `td_orders_sync` or `uc_orders_sync`, and every `orders_sync_log` insert/update must include that resolved `pipeline_id`.

---

# **B) Pipeline parameterization**

Applies to:

* `td_orders_sync`
* `uc_orders_sync`

Remove hardcoded “last 90 days” logic from core code.

Both pipelines must accept:

```bash
--from-date YYYY-MM-DD
--to-date   YYYY-MM-DD
```

Validation:

```
from_date <= to_date
```

Fallback defaults if omitted:

```
from_date = current_date - 89
to_date   = current_date
```

TD flags must continue to work:

```
--orders-only
--sales-only
```

Pipeline ID resolution:

```sql
SELECT id FROM pipelines WHERE code='td_orders_sync';
SELECT id FROM pipelines WHERE code='uc_orders_sync';
```

TD must use `config.pipeline_timezone` for all date calculations (no UTC/default `current_date` behavior).

---

# **C) Logging rules inside pipelines**

At start of each **store + date window**:

Insert:

```
status = 'running'
pipeline_id
run_id
run_env
store_code
cost_center
from_date
to_date
```

### TD (`td_orders_sync`)

| Event         | Action                         |
| ------------- | ------------------------------ |
| Orders pulled | set `orders_pulled_at = now()` |
| Sales pulled  | set `sales_pulled_at = now()`  |

Final status:

| Condition             | status    |
| --------------------- | --------- |
| both succeed          | `success` |
| orders ok, sales fail | `partial` |
| orders fail           | `failed`  |

On exception → set `status` + `error_message`.

### UC (`uc_orders_sync`)

GST is treated as order data.

| Event      | Action                         |
| ---------- | ------------------------------ |
| GST pulled | set `orders_pulled_at = now()` |

Final status:

```
success | failed
```

`sales_pulled_at` remains NULL.

---

# **D) Orchestrator — `app/crm_downloader/orders_sync_run_profiler/main.py`**

## CLI

```bash
--env <string>
--sync-group TD|UC|ALL     (default ALL)
--window-days N           (default 90)
--overlap-days K          (default 1)
--max-workers M          (default 2 or 3)
--force
--store-code <code>
```

| Flag           | Meaning                                                    |
| -------------- | ---------------------------------------------------------- |
| `window-days`  | Size of each backfill chunk                                |
| `overlap-days` | How many most-recent successful days must always be re-run |
| `max-workers`  | Parallelism across stores                                  |
| `--force`      | Ignore success windows and re-run everything               |

---

## On start

Insert into `pipeline_run_summaries`:

```
pipeline_name = 'orders_sync_run_profiler'
run_id = unique
run_env
started_at = now()
report_date = current_date
overall_status = 'running'
```

---

## Store selection

```sql
SELECT *
FROM store_master
WHERE is_active = true
AND start_date IS NOT NULL
AND store_code IS NOT NULL
```

Filtered by `--sync-group` and `store_master.store_code`, with support for explicit `--store-code` selection (exact match on `store_master.store_code`).

---

# **AUTO WINDOW DECISION LOGIC**

Definitions:

```
today = current_date (computed in config.pipeline_timezone)
window_days = N
overlap_days = K
```

A **success window** is any `orders_sync_log` row where:

```
pipeline_id + store_code + from_date + to_date AND status='success'
```

---

## 1) Determine starting point (per store + pipeline)

```
If no success windows exist:
    next_from = store_master.start_date
Else:
    last_success_to = MAX(to_date) where status='success'
    next_from = max(
        store_master.start_date,
        last_success_to - (overlap_days - 1)
    )
```

This guarantees late-day re-ingestion
(e.g., Jan-6 7pm success → Jan-6 re-runs next time).

---

## 2) Build windows

```
while next_from <= today:
    next_to = min(next_from + (window_days - 1), today)
```

---

## 3) Window execution rule

Execute a window if:

```
--force
OR
window overlaps overlap range
OR
no success exists for (pipeline_id, store_code, from_date, to_date)
```

Overlap windows always run, even if already successful.

---

## 4) Parallel execution model

Each job:

```
(store_code, pipeline_id, from_date, to_date)
```

Rules:

* Max `--max-workers` running at once
* A store is locked so only one job runs for it at a time
* Windows for a store execute sequentially
* Different stores and pipelines may run in parallel

Locks must be implemented via PostgreSQL advisory locks or filesystem locks.

---

## 5) Failure handling

For a store + pipeline:

* If a window is **failed or partial** → stop further windows for that store in this run
* Continue with other stores

No infinite retry in a single run.

---

## 6) Today refresh

If **no scheduled window already includes today**:

```
today_from = today
today_to   = today
```

Run if:

```
--force OR no success exists for (today, today)
```

---

# **E) End-of-run**

Update `pipeline_run_summaries`:

```
finished_at = now()

overall_status =
    success  if all windows succeeded
    failed   if any failed
    partial  if only partials

summary_text =
    per-pipeline success / partial / failed counts
```

---

# **F) Shell wrapper**

`scripts/orders_sync_run_profiler.sh`

```bash
exec poetry run python -m app.crm_downloader.orders_sync_run_profiler.main "$@"
```
