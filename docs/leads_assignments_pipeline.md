# TSV – Leads Assignment Pipeline (`app/lead_assignment`)

**Goal for Codex:**  
Implement a new, self-contained `leads_assignment` pipeline under `app/lead_assignment` that:
 - Selects eligible missed leads,
 - Assigns them to agents based on store+mapping rules and per-lot limits,
 - Persists assignments,
 - Generates per-store/per-agent PDF sheets for agents (Row1 = read-only, Row2 = input),
 - Registers these PDFs in `documents`,
 - Integrates with the existing `pipelines` + `notification_*` framework to email PDFs,
 - Can be run both independently and from the existing single-session pipeline.

**IMPORTANT:**  
 - Do **NOT** refactor unrelated modules.  
 - Do **NOT** add complex multi-level failover/retry logic.  
 - Keep logic simple, explicit, and fully implemented end-to-end.  
 - At the end, add explicit sanity checks as described in the **Verification Checklist**.

---

## 1. Project Context

Existing modules:

- `app/dashboard_downloader`
- `app/crm_downloader`

You must create a new module:

- `app/lead_assignment`

There is an existing infrastructure for:

- **Pipelines**: `pipelines`
- **Notification Profiles**: `notification_profiles`
- **Notification Recipients**: `notification_recipients`
- **Documents**: `documents`

You must follow the same patterns used by `dashboard_downloader` for:

- DB access (sessions, transactions),
- Logging,
- Notification handling,
- Integration with the orchestrator entrypoint.

**Do not break or alter their behavior.**

---

## 2. Database Schema Changes

> Implement these using the project’s existing migration mechanism (e.g., Alembic), **not** raw manual modifications inside application code.  
> If in doubt, inspect how previous migrations were written and follow that style.

### 2.1 `store_master` – add assignment flag

Add a flag to identify which stores participate in lead assignment:

```sql
ALTER TABLE store_master
ADD COLUMN IF NOT EXISTS assign_leads boolean NOT NULL DEFAULT false;
````


# TSV – Leads Assignment Pipeline (`app/lead_assignment`)

> **Goal for Codex**
> Implement a new, self-contained `leads_assignment` pipeline under `app/lead_assignment` that:
>
> * Selects eligible missed leads.
> * Assigns them to agents based on store + mapping rules and per-lot limits.
> * Persists assignments in dedicated tables.
> * Generates per-store/per-agent PDF sheets for agents (Row 1 = read-only, Row 2 = input).
> * Registers these PDFs in `documents`.
> * Integrates with the existing `pipelines` + `notification_*` framework to email PDFs.
> * Can be run both independently and from the existing single-session pipeline.
>
> **Important constraints**
>
> * Do **NOT** refactor unrelated modules.
> * Do **NOT** add complex failover/retry layers.
> * Keep logic simple, explicit, and fully implemented end-to-end.
> * At the end, satisfy the **Verification Checklist** at the bottom.

---

## 1. Project Context

Existing application modules:

* `app/dashboard_downloader`
* `app/crm_downloader`

New module to create:

* `app/lead_assignment`

Existing infrastructure you must reuse:

* Pipelines: `public.pipelines`
* Notification profiles: `public.notification_profiles`
* Notification recipients: `public.notification_recipients`
* Documents: `public.documents`

Follow the same patterns used by the existing pipelines for:

* DB access + sessions + transactions.
* Logging and JSON-style status logs.
* Notification orchestration and email delivery.

Do **not** change behaviour of existing pipelines.

---

## 2. Database Schema Changes

Use the existing migration mechanism (e.g. Alembic).
Do **not** hardcode schema creation in application logic.

### 2.1 `store_master` – add assignment flag

Add a flag to indicate which stores participate in lead assignment:

```sql
ALTER TABLE store_master
ADD COLUMN IF NOT EXISTS assign_leads boolean NOT NULL DEFAULT false;
```

Semantics:

* `assign_leads = false` → store’s missed leads are **never** assigned.
* `assign_leads = true` → store is eligible (if mapping exists).

---

### 2.2 `agents_master` – new table

Create a master table for agents / agent groups:

```sql
CREATE TABLE IF NOT EXISTS agents_master (
    id            bigserial PRIMARY KEY,
    agent_code    char(4) UNIQUE,               -- e.g. '0001'
    agent_name    varchar(32) NOT NULL,
    mobile_number varchar(16) NOT NULL,
    is_active     boolean NOT NULL DEFAULT true
);
```

---

### 2.3 `store_lead_assignment_map` – new table

Per-store → per-agent mapping + caps:

```sql
CREATE TABLE IF NOT EXISTS store_lead_assignment_map (
    id                   bigserial PRIMARY KEY,
    store_code           varchar NOT NULL,
    agent_id             bigint NOT NULL REFERENCES agents_master(id),

    is_enabled           boolean NOT NULL DEFAULT true,
    priority             int NOT NULL DEFAULT 100,  -- lower = higher priority (future use)

    max_existing_per_lot int,                      -- max existing-customer leads per run
    max_new_per_lot      int,                      -- max new-customer leads per run
    max_daily_leads      int,                      -- total cap per day for this (store, agent)

    UNIQUE (store_code, agent_id)
);
```

---

### 2.4 `missed_leads` – Existing Table, add assignment flag

Existing (simplified) structure given here for your references purposes, table with a lot of rows already exists in the system:

```sql
CREATE TABLE IF NOT EXISTS public.missed_leads
(
    pickup_row_id        integer PRIMARY KEY,
    mobile_number        varchar NOT NULL,
    pickup_no            varchar,
    pickup_created_date  date,
    pickup_created_time  varchar,
    store_code           varchar NOT NULL,
    store_name           varchar,
    pickup_date          date,
    pickup_time          varchar,
    customer_name        varchar,
    special_instruction  varchar,
    source               varchar,
    final_source         varchar,
    customer_type        varchar,
    is_order_placed      boolean,
    run_id               varchar(64),
    run_date             date,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_missed_leads_store_mobile UNIQUE (store_code, mobile_number)
);
```

Revision `0021_add_timestamp_ingest_tables_with_missed_leads` also backfills the
`created_at`/`updated_at` columns across ingest tables (including
`missed_leads`) so ORM defaults and ingest pipelines stay aligned.

Add:

```sql
ALTER TABLE missed_leads
ADD COLUMN IF NOT EXISTS lead_assigned boolean NOT NULL DEFAULT false;
```

Semantics:

* `lead_assigned = false` → lead can be assigned.
* `lead_assigned = true` → lead has already been assigned (must not be assigned again).

---

### 2.5 `lead_assignment_batches` – new table

Represents one execution (run) of the leads assignment pipeline:

```sql
CREATE TABLE IF NOT EXISTS lead_assignment_batches (
    id          bigserial PRIMARY KEY,
    batch_date  date NOT NULL,             -- typically current_date
    triggered_by text,                     -- e.g. 'pipeline:single_session', 'manual'
    run_id      text,                      -- optional link to upstream run
    created_at  timestamptz NOT NULL DEFAULT now()
);
```

---

### 2.6 `lead_assignments` – new table

Link between a missed lead and an agent, plus PDF identity and snapshot data:

```sql
CREATE TABLE IF NOT EXISTS lead_assignments (
    id                    bigserial PRIMARY KEY,
    assignment_batch_id   bigint NOT NULL REFERENCES lead_assignment_batches(id),
    lead_id               integer NOT NULL REFERENCES missed_leads(pickup_row_id),
    agent_id              bigint NOT NULL REFERENCES agents_master(id),

    page_group_code       text NOT NULL,    -- 'L{YYMMDD}{agent_code}' e.g. 'L2501290001'
    rowid                 int  NOT NULL,    -- 1..N within that page_group_code
    lead_assignment_code  text NOT NULL,    -- page_group_code || '-' || LPAD(rowid, 4, '0')

    -- Snapshot for PDF Row 1:
    store_code            varchar NOT NULL,
    store_name            varchar,
    lead_date             date,            -- from pickup_date / pickup_created_date / run_date
    lead_type             char(1),         -- 'E' (Existing) or 'N' (New) from customer_type
    mobile_number         varchar NOT NULL, --from mobile_number
    cx_name               varchar,         -- from customer_name
    address               varchar,         -- from special_instruction or blank
    lead_source           varchar,         -- final_source or source

    assigned_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_lead_assignments_code UNIQUE (lead_assignment_code),
    CONSTRAINT uq_lead_assignments_lead UNIQUE (lead_id)
);
```

Rules (to implement in code):

* `lead_type`:

  * `'N'` if `customer_type ILIKE 'New%'`.
  * Otherwise `'E'`.
* `lead_date` = first non-null of:
  
  * `pickup_created_date`,
  * `run_date`.

---

### 2.7 `lead_assignment_outcomes` – new table

Stores the agent’s filled data from the PDF (Row 2):

```sql
CREATE TABLE IF NOT EXISTS lead_assignment_outcomes (
    id                       bigserial PRIMARY KEY,
    lead_assignment_id       bigint NOT NULL UNIQUE
                             REFERENCES lead_assignments(id),

    converted_flag           boolean,
    order_number             varchar,
    order_date               date,
    order_value              numeric(12,2),

    payment_mode             varchar,
    payment_amount           numeric(12,2),

    remarks                  text,

    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);
```

---

### 2.8 `documents` – use for leads assignment PDFs

Use the existing `documents` structure (no schema change). You must insert rows here for each generated PDF.

Conventions for leads assignment PDFs:

* `doc_type = 'leads_assignment'`
* `doc_subtype = 'per_store_agent_pdf'`
* `doc_date = batch_date` from `lead_assignment_batches`
* `reference_name_1 = 'pipeline'`, `reference_id_1 = 'leads_assignment'`
* `reference_name_2 = 'store_code'`, `reference_id_2 = store_code`
* `reference_name_3 = 'agent_code'`, `reference_id_3 = agent_code`
* `mime_type = 'application/pdf'`
* `storage_backend = 'fs'`
* `file_path` = path where the file is stored on disk
* `status = 'ok'`
* `created_by = 'leads_assignment_pipeline'` (fixed string)

---

## 3. Pipeline + Notification Integration

### 3.1 `pipelines` row

Ensure there is a row in `pipelines`:

```sql
INSERT INTO pipelines (code, description)
VALUES ('leads_assignment', 'Leads Assignment')
ON CONFLICT (code) DO NOTHING;
```

Use its `id` as `pipeline_id` in notification profiles.

---

### 3.2 `notification_profiles` row

Add a notification profile for the leads assignment pipeline:

```sql
INSERT INTO notification_profiles (
    pipeline_id,
    code,
    description,
    env,
    scope,
    attach_mode,
    is_active
) VALUES (
    (SELECT id FROM pipelines WHERE code = 'leads_assignment'),
    'leads_assignment',
    'Assign leads as and when needed',
    'any',
    'store',           -- store-scoped
    'per_store_pdf',   -- one PDF per store/agent grouping
    true
)
ON CONFLICT (pipeline_id, code, env) DO NOTHING;
```

Respect existing `CHECK` constraints on `scope`, `attach_mode`, `env`.

---

### 3.3 `notification_recipients` rows (runtime usage)

There will be rows in `notification_recipients` with:

* `profile_id` = this `leads_assignment` notification profile.
* `store_code` = store_master.store_code where `assign_leads = true`.
* `env = 'any'`.
* `email_address = 'wagid.sheikh@gmail.com'` (for now).
* `display_name = 'Wagid Sheikh'`.
* `send_as = 'to'`.
* `is_active = true`.

Your task: **use** these rows at runtime (do not seed them programmatically).
Use the same notification/email orchestration logic as existing pipelines.

---

## 4. New Module: `app/lead_assignment`

Create the module with at least these files:

* `app/lead_assignment/__init__.py`
* `app/lead_assignment/assigner.py`
* `app/lead_assignment/pdf_generator.py`
* `app/lead_assignment/runner.py` (or `main.py` as the pipeline entrypoint)

Follow folder and import conventions from `dashboard_downloader` / `crm_downloader`.

---

### 4.1 `assigner.py` – core assignment logic

Implement a main function, for example:

```python
def run_leads_assignment(
    db_session,
    triggered_by: str,
    run_id: str | None = None,
) -> int:
    """
    1. Create a lead_assignment_batches row.
    2. Fetch eligible leads from missed_leads, joined with store + mapping + agent.
    3. Apply:
       - store_master.assign_leads,
       - store_lead_assignment_map.is_enabled,
       - agents_master.is_active,
       - lead_assigned = false,
       - is_order_placed = false or NULL.
    4. Apply per-lot caps:
       - max_existing_per_lot (lead_type='E'),
       - max_new_per_lot (lead_type='N'),
       - max_daily_leads (includes already assigned today).
    5. For each assigned lead:
       - Generate page_group_code, rowid, lead_assignment_code.
       - Insert into lead_assignments with snapshot fields.
       - Set missed_leads.lead_assigned = true.
    6. Return batch_id.
    """
```

#### 4.1.1 Eligibility query (conceptual)

Equivalent SQL:

```sql
SELECT
    ml.*,
    sm.assign_leads,
    slam.agent_id,
    am.agent_code
FROM missed_leads ml
JOIN store_master sm
  ON sm.store_code = ml.store_code
JOIN store_lead_assignment_map slam
  ON slam.store_code = ml.store_code
 AND slam.is_enabled = true
JOIN agents_master am
  ON am.id = slam.agent_id
WHERE sm.assign_leads = true
  AND ml.lead_assigned = false
  AND (ml.is_order_placed = false OR ml.is_order_placed IS NULL)
ORDER BY ml.pickup_created_date DESC;
```

In Python:

* Group by `(store_code, agent_id)` for applying per-lot caps.
* For `lead_type`:

  * `'N'` if `customer_type ILIKE 'New%'`,
  * else `'E'`.

#### 4.1.2 Respecting caps

Per `(store_code, agent_id)`:

* Determine how many leads were already assigned **today**:

  * Query `lead_assignments` with `lead_date` or `assigned_at::date = current_date`.
* Compute remaining quota for `max_daily_leads`.
* While iterating leads in `pickup_created_date DESC` order:

  * Track counts for:

    * `assigned_existing_count` (type 'E'),
    * `assigned_new_count` (type 'N'),
    * `assigned_total_count`.
  * Stop assigning when any of:

    * `assigned_existing_count == max_existing_per_lot` (for 'E'),
    * `assigned_new_count == max_new_per_lot` (for 'N'),
    * `assigned_total_count == max_daily_leads`.

All values may be `NULL` in DB (meaning “no limit”); handle that by skipping that specific cap in such cases.

#### 4.1.3 Page group + rowid + code

For each `(store_code, agent_id)`:

* `today_yymmdd = current_date.strftime('%y%m%d')`.
* `page_group_code = f"L{today_yymmdd}{agent_code}"`
  (agent_code is char(4); use it as-is).
* Maintain a `row_counter` starting at 0.
* For each selected lead in that group:

  * `row_counter += 1`
  * `rowid = row_counter`
  * `lead_assignment_code = f"{page_group_code}-{rowid:04d}"`.

#### 4.1.4 Snapshot data mapping

For each `missed_leads` row, populate `lead_assignments` fields:

* `lead_id = pickup_row_id`
* `store_code = ml.store_code`
* `store_name = ml.store_name`
* `lead_date = pickup_date or pickup_created_date or run_date`
* `lead_type = 'N' if ml.customer_type ILIKE 'New%' else 'E'`
* `mobile_number = ml.mobile_number`
* `cx_name = ml.customer_name`
* `address = ml.special_instruction` (or `None`)
* `lead_source = ml.final_source or ml.source`

After inserting, update:

```sql
UPDATE missed_leads
SET lead_assigned = true
WHERE pickup_row_id = :lead_id;
```

Wrap the whole assignment in a transaction and commit at the end.
On error, rollback and raise/log clearly.
Do **not** silently ignore failures.

---

### 4.2 `pdf_generator.py` – PDF creation + documents rows

Implement a function, e.g.:

```python
def generate_pdfs_for_batch(db_session, batch_id: int) -> list[int]:
    """
    For the given batch_id:
      - Fetch lead_assignments.
      - Group by (store_code, agent_id, page_group_code).
      - Generate one PDF per group.
      - Save PDFs to filesystem.
      - Insert rows into documents.
      - Return list of document IDs created.
    """
```

#### 4.2.1 Query data

```sql
SELECT la.*, am.agent_code, am.agent_name, lab.batch_date
FROM lead_assignments la
JOIN agents_master am ON am.id = la.agent_id
JOIN lead_assignment_batches lab ON lab.id = la.assignment_batch_id
WHERE la.assignment_batch_id = :batch_id
ORDER BY la.store_code, la.agent_id, la.rowid;
```

Group in Python by `(store_code, agent_id, page_group_code)`.

#### 4.2.2 PDF layout

For each group:

Header (top of PDF):

* `Page Group Code: {page_group_code}`
* `Agent: {agent_code} - {agent_name}`
* `Batch Date: {batch_date}`

Body:

For each `lead_assignments` row:

* **Row 1 (read-only, printed from DB):**

  * RowID → `rowid`
  * Lead Date → `lead_date`
  * Lead Type (E/N) → `lead_type`
  * Mobile No → `mobile_number`
  * Customer Name → `cx_name`
  * Address → `address`
* **Row 2 (blank input row for agent to fill digitally - using ReportLab & Acroform):**

  * Conv (Y/N)
  * Order No
  * Order Date
  * Value
  * Payment Mode
  * Payment Amt
  * Remarks

Use the same PDF library already used in the project (ReportLab, AcroForm).
Keep the table structure **simple and consistent**, no fancy formatting.

#### 4.2.3 Filesystem + documents insertion

For each generated PDF:

* File name suggestion:
  `leads_assignment_{batch_date}_{store_code}_{agent_code}.pdf`
* Path suggestion (adapt to project conventions):
  `reports/leads_assignment/{YYYY-MM-DD}/{file_name}`

Insert a record into `documents` using the conventions from section 2.8.

Return the list of `documents.id` created.

---

### 4.3 `runner.py` – pipeline entrypoint

Implement a top-level function, e.g.:

```python
def run_leads_assignment_pipeline(env: str, run_id: str | None = None) -> None:
    """
    1. Open DB session with existing helper.
    2. Call run_leads_assignment(...) to create a batch and assignments.
    3. If batch has 0 assignments:
         - Log and exit gracefully (no PDFs, no emails).
    4. Else:
         - Call generate_pdfs_for_batch(...) to create documents.
         - Use existing notification/email framework to:
           - Find notification profile for 'leads_assignment'.
           - Resolve recipients by store_code and env.
           - Attach the relevant documents (per_store_pdf).
           - Send summary email (similar to other pipelines).
    5. Record status logs (JSON) in the same style as other pipelines.
    """
```

Requirements:

* This function must be callable in two ways:

  * From the existing single-session pipeline at the end (import and call).
  * From a CLI / orchestrator as a standalone `leads_assignment` run.
* Respect the current logging format (e.g. `{"phase": "...", "status": "..."}`) used elsewhere.
* Do not add cron logic inside the module. Scheduling is external.

---

## 5. Business Rules (Summary)

Codex must ensure all these rules are honoured:

1. **Eligibility**

   * `store_master.assign_leads = true`.
   * `store_lead_assignment_map.is_enabled = true`.
   * `agents_master.is_active = true`.
   * `missed_leads.lead_assigned = false`.
   * `missed_leads.is_order_placed = false` or `NULL`.
2. **Sorting**

   * Fetch eligible leads ordered by `pickup_created_date DESC` (most recent first).
3. **Caps**

   * `max_existing_per_lot` → limit on `lead_type = 'E'`.
   * `max_new_per_lot` → limit on `lead_type = 'N'`.
   * `max_daily_leads` → limit across both types for that `(store_code, agent_id, batch_date)`.
4. **Idempotence**

   * Once a lead is assigned:

     * `missed_leads.lead_assigned = true`.
     * `lead_assignments` has a unique row `UNIQUE (lead_id)`.
   * Re-running the assignment in the same day must **not** reassign the same lead.
5. **PDF Behaviour**

   * One PDF per `(store_code, agent_id, batch_id)` group.
   * Each logical lead uses 2 physical rows:

     * Row 1 = read-only snapshot.
     * Row 2 = blank input.
   * Each PDF has a row in `documents`.
6. **Notifications**

   * Use `pipelines`, `notification_profiles`, `notification_recipients`.
   * For profile `code='leads_assignment'`, scope `store`, attach mode `per_store_pdf`:

     * Attach appropriate PDFs for each store.
   * Respect `env` and `is_active` filters on recipients.

---

## 6. Verification Checklist (Codex MUST satisfy)

Before declaring this done, verify:

1. **Migrations**

   * All new tables/columns exist.
   * Migrations run cleanly on an existing DB.
2. **Assignment Logic**

   * With test data in `missed_leads`, `store_master`, `store_lead_assignment_map`, `agents_master`:

     * Running the pipeline creates `lead_assignment_batches` and `lead_assignments`.
     * `missed_leads.lead_assigned` becomes `true` for assigned rows.
   * Running the pipeline again immediately does not create duplicate assignments for the same leads.
3. **Caps Behaviour**

   * With small caps (e.g., 1 existing, 1 new, 2 daily), confirm the limits are enforced.
4. **PDF Generation**

   * For a sample batch:

     * PDFs are created on disk.
     * Layout is correct (Row 1 = read-only, Row 2 = input).
   * Corresponding rows exist in `documents` with correct metadata.
5. **Notification Integration**

   * With a `notification_profiles` row for `leads_assignment` and at least one `notification_recipients` row:

     * Running the pipeline sends emails using the existing notification system.
     * Correct PDFs are attached.
6. **Independence**

   * The pipeline can be run:

     * On its own.
     * From within the single-session pipeline.
   * No unwanted side effects on existing pipelines.
7. **Non-intrusiveness**

   * No behavioural changes to `dashboard_downloader`, `crm_downloader`, or other modules.
   * No new global config that breaks current behaviours.

If any part cannot be implemented as described due to existing project constraints, leave a **short, explicit comment** explaining why, and implement the closest safe behaviour.
