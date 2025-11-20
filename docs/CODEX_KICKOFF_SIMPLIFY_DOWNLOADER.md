# Codex Kickoff — app-based CRM backend (Finalize & Implement)

**Follow `/docs/CODEX_DEV_PROCESS.md` for the gated workflow.** This file is the *actual work spec* Codex must implement. The project now ships as the `app`-based CRM backend; any references to `simplify_downloader` are legacy labels kept for historical clarity.

> Non‑negotiables: keep the existing downloader & merger intact; enhance and integrate the rest.

---

## Scope — Deliver in One Pass

* **Store selection** — resolve ingestion scope from `store_master.etl_flag = TRUE` and reporting scope from `store_master.report_flag = TRUE`.
* **Automatic pipeline**: **download → merge → async ingest (Postgres) → audit counts → conditional cleanup**.
* **Boolean rule**: `is_order_placed` → `1=True`, `0=False`, **anything else=False**.
* **DB via SQLAlchemy 2.0 async (asyncpg) + Pydantic validation** (advisable) + **Alembic** (standard: create tables, revisions, upgrades).
* **Dockerized runtime (server)**: **app + Postgres both run inside Docker** (compose). No public DB port. Mac connects via SSH tunnel.
* **GitHub CI/CD** (build/test + deploy via SSH/compose).
* Implement structured **JSON logger** (`json_logger.py`) and use it across the pipeline.

Do **not** rewrite the working downloader/merger. Enhance & integrate.

---

## Real CSV Schemas (map only; do not alter merge)

* **missed_leads** (e.g., `pickup_*.csv`):

```
id, mobile_number, pickup_no, pickup_created_date, pickup_created_time,
store_code, store_name, pickup_date, pickup_time, customer_name,
special_instruction, source, final_source, customer_type, is_order_placed
```

* **undelivered_all** (e.g., `undelieverd_order_*.csv`):

```
order_id, order_date, store_code, store_name, taxable_amount, net_amount,
service_code, mobile_no, status, customer_id, expected_deliver_on, actual_deliver_on
```

* **repeat_customers** (e.g., `repeat*.csv`):

```
Store Code, Mobile No., Status
```

---

## Config Extensions (no breaking changes)

Keep `FILE_SPECS` exactly as‑is. Extend only:

```python
# Output names for merged buckets
MERGED_NAMES = {
    "missed_leads": f"merged_missed_leads_{YMD_TODAY}.csv",
    "undelivered_all": f"merged_undelivered_all_{YMD_TODAY}.csv",
    "repeat_customers": f"merged_repeat_customers_{YMD_TODAY}.csv",
}

# DB spec per merge bucket (case‑insensitive CSV header mapping)
MERGE_BUCKET_DB_SPECS = {
    "missed_leads": {
        "table_name": "missed_leads",
        "dedupe_keys": ["store_code", "pickup_no"],
        "column_map": {
            "id": "pickup_row_id",
            "mobile_number": "mobile_number",
            "pickup_no": "pickup_no",
            "pickup_created_date": "pickup_created_date",
            "pickup_created_time": "pickup_created_time",
            "store_code": "store_code",
            "store_name": "store_name",
            "pickup_date": "pickup_date",
            "pickup_time": "pickup_time",
            "customer_name": "customer_name",
            "special_instruction": "special_instruction",
            "source": "source",
            "final_source": "final_source",
            "customer_type": "customer_type",
            "is_order_placed": "is_order_placed",
        },
        "coerce": {
            "pickup_row_id": "int",
            "mobile_number": "str",
            "pickup_no": "str",
            "pickup_created_date": "date",
            "pickup_created_time": "str",
            "store_code": "str",
            "store_name": "str",
            "pickup_date": "date",
            "pickup_time": "str",
            "customer_name": "str",
            "special_instruction": "str",
            "source": "str",
            "final_source": "str",
            "customer_type": "str",
            "is_order_placed": "bool",  # 1=True; 0/other=False
        },
    },

    "undelivered_all": {
        "table_name": "undelivered_orders",
        "dedupe_keys": ["store_code", "order_id"],
        "column_map": {
            "order_id": "order_id",
            "order_date": "order_date",
            "store_code": "store_code",
            "store_name": "store_name",
            "taxable_amount": "taxable_amount",
            "net_amount": "net_amount",
            "service_code": "service_code",
            "mobile_no": "mobile_no",
            "status": "status",
            "customer_id": "customer_id",
            "expected_deliver_on": "expected_deliver_on",
            "actual_deliver_on": "actual_deliver_on",
        },
        "coerce": {
            "order_id": "str",
            "order_date": "date",
            "store_code": "str",
            "store_name": "str",
            "taxable_amount": "float",
            "net_amount": "float",
            "service_code": "str",
            "mobile_no": "str",
            "status": "str",
            "customer_id": "str",
            "expected_deliver_on": "date",
            "actual_deliver_on": "date",
        },
    },

    "repeat_customers": {
        "table_name": "repeat_customers",
        "dedupe_keys": ["store_code", "mobile_no"],
        "column_map": {
            "Store Code": "store_code",
            "Mobile No.": "mobile_no",
            "Status": "status",
        },
        "coerce": {
            "store_code": "str",
            "mobile_no": "str",
            "status": "str",
        },
    },
}
```

**Boolean coercion:** implement `bool` as `"1"→True`, `"0"→False`, **everything else → False**.

---

## Store selection input

* Resolve ingestion stores from `store_master.etl_flag = TRUE` and ensure at least one store is eligible; exit non‑zero with a clear message if none are flagged.
* Reporting pipelines rely on `store_master.report_flag = TRUE` and validate that every reporting store is also present in the ingestion scope.

---

## ORM + Pydantic + Alembic

* Use **SQLAlchemy 2.x async** (asyncpg) for DB I/O.
* Define **ORM models** for each bucket/table based on `MERGE_BUCKET_DB_SPECS`.
* Define **Pydantic models** for row validation/coercion (date/float/int + bool rule).
* Implement **Alembic** (standard):

  * Initial revision matches the ORM models (PK/UNIQUE per `dedupe_keys`).
  * Future diffs via normal `revision --autogenerate` + `upgrade`.
  * Alembic `env.py` must convert `postgresql+asyncpg://` to `postgresql://` for the engine.

> Do **not** include raw DDL here. Migrations are managed by ORM + Alembic.

---

## Async Ingestion (automatic, after each merge)

* Trigger ingestion per merged file.
* Stream CSV → batch (default `INGEST_BATCH_SIZE=3000`, env‑overridable).
* Case‑insensitive header match; map via `column_map`; unknown columns → warn & skip.
* Coerce via `coerce` (bool rule above).
* Upsert with Postgres dialect: `ON CONFLICT (dedupe_keys) DO UPDATE SET <non‑key cols>`.
* **Counters captured**:

  * `download_rows_by_store` (per individual CSV)
  * `merged_rows` (post‑dedupe)
  * `ingested_rows` (inserted + updated)
* Fault tolerant: on a bucket failure, log error; continue others.

---

## Audit Counts & Conditional Cleanup

For each bucket & run date:

1. Sum **individual CSV** row counts (download rows).
2. Compute **merged CSV** rows **after de‑duplication**.
3. Count **ingested rows** (rows affected) for that merged set.

* Log all three via **json_logger**.
* **If** `merged_rows == ingested_rows` **and** equals the unique expected per merge logic → delete the **individual CSVs** and the **merged CSV** (atomic, safe). Log cleanup.
* Else retain files; log discrepancy & reason.

---

## JSON Logger (`json_logger.py`) — Implement & Use

* Output newline‑delimited JSON to stdout (optional file sink via env/setting).
* Common fields:

  * `ts`, `run_id`, `phase` (`download`/`merge`/`ingest`/`audit`/`cleanup`/`error`)
  * `bucket`, `store_code` (if applicable)
  * `date`, `merged_file`
  * `counts` (`download_total`, `merged_rows`, `ingested_rows`)
  * `status` (`ok|warn|error`), `message`, `duration_ms`, `extras` (dict)
* Replace/wrap current logging in downloader/merger/ingester to emit these.

---

## Dockerized Runtime (server)

* Provide `docker-compose.yml` with services:

  * **db**: `postgres:16`, named volume, healthcheck. **No port published**.
  * **app**: build from repo; run `python -m app ...` inside container (legacy: `python -m simplify_downloader ...`).
* Execution example (from server):

  ```bash
  docker compose run --rm app python -m app run
  ```
* **Mac access (PGAdmin4)**: create SSH tunnel to server and connect to `localhost:5432`.

  ```bash
  ssh -N -L 5432:127.0.0.1:5432 user@your-server
  ```

  (Do not expose 5432 publicly.)

---

## GitHub CI/CD

* **CI (`.github/workflows/ci.yml`)**: checkout → setup Python → install deps → lint/type‑check/tests.
* **Deploy (`.github/workflows/deploy-prod.yml`)**: on push to `main` or manual → SSH to server → `git pull` → `docker compose pull` → `docker compose build app` → `docker compose up -d db` (wait healthy) → `docker compose run --rm app python -m app db upgrade` (legacy: `python -m simplify_downloader`).

---

## CLI & Env

* CLI flags:

  * `--dry_run` (skip DB writes; log ingestion plan & sample rows)
* Env:

  * `DATABASE_URL` (required for ingestion/Alembic)
  * `INGEST_BATCH_SIZE` (optional; default 3000)
  * Reuse existing env names elsewhere.

---

## Acceptance Criteria

* Pipeline runs end‑to‑end inside Docker: **download → merge → ingest → audit → cleanup**.
* Store selection derived from `store_master.etl_flag` and `store_master.report_flag`; downloader/merger unchanged except for wiring and counters.
* `is_order_placed` coercion enforced (`1=True`, `0/other=False`).
* ORM models + Pydantic models match `MERGE_BUCKET_DB_SPECS`.
* Alembic manages schema; initial revision matches models; later changes via normal revisions.
* Ingestion uses async SQLAlchemy/asyncpg with UPSERT + batching.
* JSON logs show counts, outcomes, and cleanup decisions.
* If counts align, individual & merged CSVs removed; otherwise retained with discrepancy logs.
* CI runs; deploy builds `app`, brings up `db`, applies Alembic upgrade, ready to execute.

---

## Guardrails

* Do **not** change existing file paths, filenames, or merge semantics (beyond computing unique merged counts using the same dedupe keys).
* Headers are matched **case‑insensitively**; unknown columns → warn & skip.
* Never abort entire run for one failing bucket or store; log and proceed.
* All logs go through `json_logger.py` and include a `run_id` per execution.
