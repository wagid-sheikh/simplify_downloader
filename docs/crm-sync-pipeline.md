# CRM Sync/Download Pipeline Specifications

## Key Notes to always remember

### PROTECTED PATHS (DO NOT MODIFY unless explicitly instructed):

- app/dashboard_downloader/**
- app/lead_assignment/**

If you believe a change is required under protected paths, stop and ask for approval first.
Any PR that touches protected paths must be isolated and explicitly labeled “protected-path-change”.

### DOs & DON'Ts

* DO:
  - Use existing helpers from app/dashboard_downloader for:
  - DB session
  - run_id / run_date
  - pipeline run logging
  - notifications
  - Keep all new ETL logic for TD/UC/bank under app/crm_downloader (and app/bank_sync, app/reports where applicable).
  - All Date/Time to be Zoned as per environment setting (already exists) `PIPELINE_TIMEZONE="Asia/Kolkata"`
  - All navigation wait time to adhere to system_config.key=DASHBOARD_DOWNLOAD_NAV_TIMEOUT
* DON’T:
  - Don’t copy-paste working code and modify in-place inside dashboard_downloader, lead_assignment.
  - Don’t introduce new environment variables for things already handled by store_master or existing config.
  - Don’t create new notification mechanisms; reuse notification_profiles, email_templates, notification_recipients.
* Failure / partial success handling
  - Each pipeline run must log status per store into the appropriate log tables.
  - If all stores fail → set `pipeline_run_summaries.status = 'error'`.
  - If at least one store succeeds **and** at least one fails → set `status = 'warning'`.
  - If all stores succeed → set `status = 'ok'`.
  - Never roll back successful stores because another store failed; partial loads are allowed but must be visible in logs.

### Existing working pipelines:

* There are already existing working pipelines in the system in production

  - app/dashboard_downloader
  - app/lead_assignment
* A pipeline folder for this development assignment already exists (but not developed/code written):

  - app/crm_downloader
  - app/crm_downloader/data        (temporary to hold downloaded Excel files)
  - app/crm_downloader/profiles    (Playwright storage_state JSON per store)
    - storage state json file name convention {store_code}_storage_state.json
    - When {store_code}_storage_state.json exists:
      - First attempt login using the stored session.
      - If session is invalid / expired, perform full login and overwrite the same file.
      - This profile file may be reused by all crm_downloader sub-pipelines for that store.
* Requirements for this new pipeline:

  - Do not break any existing pipelines.
  - Follow the same architectural patterns, logging style, error-handling, and DB usage as the dashboard_downloader pipeline.
  - Reuse existing helper patterns for:
    - generating run_id / run_date
    - inserting into pipelines and pipeline_run_summaries
    - sending notifications via notification_profiles + email_templates
    - Playwright browser/session management
* For any new pipeline, use the structural pattern used in `app/dashboard_downloader`:

  - Use the same kind of `main()` / orchestrator entrypoint.
  - Reuse the existing helpers for:
    - run_id + run_date generation (same function used by `dashboard_downloader`).
    - DB session/context management.
    - Recording into `pipelines` and `pipeline_run_summaries`.
    - Triggering notifications via `notification_profiles` and `email_templates`.
    - Obtaining the logger (same logger configuration used in `dashboard_downloader`).
  - Do **not** re-implement these concerns inside `crm_downloader` – always call the existing helpers.

---

## Pipelines to be developed

### td_orders_sync

- Obejctive of this pipeline is to *Download Orders Data and Sales data of `TumbleDry CRM`*
- To be developed under folder `app/crm_downloader/td_orders_sync`
- This pipeline can be invoked independently at any time (manually or by cron) so maintain strict idempotency. Cron schedule will be decided outside of this scope document.
- Input Date Range:
  - Accept two option parameters: *From Date* and *To Date*
  - If not provided → both must default to current date (`PIPELINE_TIMEZONE`)
  - Date range must drive the TD export logic (Orders + Sales) and the filtering logic in staging → production mapping
  - Idempotency: staging must be upserted using {`store_code`, `order_number`, `order_date`} keys so that re-runs do not create duplicates or data drift. Partial re-runs for the same date range must be fully supported.
- This pipeline must:
  - Download Orders and Sales data for each active store listed in `store_master` where `store_master.sync_orders_flag` = `true`
  - Use existing Playwright session management rules (probe → reuse existing {store_code}_storage_state.json OR re-login → overwrite file)
  - Push clean data into staging tables and production tables as per mapping section below
  - For multiple-store runs: allow partial success and log accurate per-store results into existing logging tables
- Deliver the following manual invocation shell scripts:
  - for run on local: `scripts/run_local_td_orders_sync.sh`
  - for run on server inside docker container: `scripts/run_prod_td_orders_sync.sh`
    - Shell scripts must:
      - Allow optional arguments: `--from-date`, `--to-date`
      - If no arguments provided → default both dates to today in `PIPELINE_TIMEZONE`
      - Follow the same invocation style as existing working scripts under dashboard_downloader (same logging, error-handling, run_id handling)

### uc_orders_sync

- Obejctive of this pipeline is to *Download Orders Data of `UClean CRM`*
- To be developed under folder `app/crm_downloader/uc_orders_sync`
- This pipeline can be invoked independently at any time (manually or by cron) so maintain strict idempotency. Cron schedule will be decided outside of this scope document.
- Input Date Range:
  - Accept two optional parameters: *From Date* and *To Date*
  - If not provided → both must default to current date (`PIPELINE_TIMEZONE`)
  - Idempotency: staging must be upserted using {`store_code`, `order_number`, `order_date`} keys so that re-runs are clean
- UC CRM does not provide a separate downloadable Sales report, but it does provide GST Report.
- Download Orders for each `store_master.sync_orders_flag` = `true` store from UClean CRM
- Use identical Playwright state management as TD logic (reuse existing {store_code}_storage_state.json, probe session validity, login on failure)
- Push clean data into staging + production tables as per mapping section below
- Partial store failures must not stop other stores → fully logged outcomes
- Deliver a shell script for manual invocation
  - for run on local: scripts/run_local_uc_sales_sync.sh
  - for run on server inside docker container: scripts/run_prod_uc_sales_sync.sh
    - Shell scripts must:
      - Allow optional arguments: `--from-date`, `--to-date`
      - If no arguments provided → default both dates to today in `PIPELINE_TIMEZONE`
      - Follow the same orchestration and logging style as TD and dashboard_downloader

### bank_sync

- Objective of this pipeline is to Ingest Bank Statement Data (for all bank accounts relevant to payment reconciliation)
- To be developed under folder `app/bank_sync`
- This pipeline can be invoked independently at any time (manually or by cron) so maintain strict idempotency. Cron schedule will be decided outside of this scope document.
- Expected source:
  - Structured bank statement CSV/Excel files manually placed in `app/bank_sync/data`
  - Expected file can be `*bank.xlsx`
- Idempotency:
  - Deduplicate on {`row_id`} excel column at staging and production levels
  - Re-runs for the same period must not create duplicates
- Responsibilities:
  - Read new/updated files from `app/bank_sync/data` folder
  - Load into staging → production tables as per defined mapping in later section
  - Status and failure behavior logged into existing pipeline run tables
- deliver a shell script for manual invocation
  - for run on local: `scripts/run_local_bank_sync.sh`
  - for run on server inside docker container: `scripts/run_prod_bank_sync.sh`
    - Shell scripts must:
      - Auto-detect all new files under `app/bank_sync/data`
      - Invoke Python orchestrator following same logging + run_id standards as other pipelines
      - Once a bank excel file is fully ingested then it must be renamed to `*bank_processed_{RUN_ID}.xlsx` and moved to a sub-folder `app/bank_sync/data/ingested` so that future runs do not pick this again

### reports

> **Phase 2 only (deferred):** All reports work (folders, sub-pipelines, scripts, and runners) is deferred to Phase 2. No reports code, migrations, or notification wiring will be delivered in the current phase; this section is retained only for planning continuity.

- Folder structure: `app/reports/{report_name}` with one orchestrator per sub-pipeline. Expected sub-pipelines (matching existing pipeline codes and notification seeds): `daily_sales_report`, `month_to_date_sales`, `daily_order_processing`, `pending_deliveries`, `pending_leads_conversion`, `package_sale_conversion`, and `pending_payment_reconciliation`.
- Idempotency: each sub-pipeline must be re-runnable; repeated runs for the same window must not duplicate output artifacts or DB writes. Use the same run_id / logging semantics as other pipelines and append-only outputs guarded by business keys when applicable.
- Orchestration contract: generate `run_id`/`run_date`, write to `pipelines`/`pipeline_run_summaries`, and emit notifications via `notification_profiles` + `email_templates` following the existing dashboard_downloader pattern.
- Scripts per report:
  - Local runners: `scripts/run_local_reports_daily_sales.sh`, `scripts/run_local_reports_month_to_date_sales.sh`, `scripts/run_local_reports_daily_order_processing.sh`, `scripts/run_local_reports_pending_deliveries.sh`, `scripts/run_local_reports_pending_leads_conversion.sh`, `scripts/run_local_reports_package_sale_conversion.sh`, `scripts/run_local_reports_pending_payment_reconciliation.sh`.
  - Prod runners: `scripts/run_prod_reports_daily_sales.sh`, `scripts/run_prod_reports_month_to_date_sales.sh`, `scripts/run_prod_reports_daily_order_processing.sh`, `scripts/run_prod_reports_pending_deliveries.sh`, `scripts/run_prod_reports_pending_leads_conversion.sh`, `scripts/run_prod_reports_package_sale_conversion.sh`, `scripts/run_prod_reports_pending_payment_reconciliation.sh` (mirror dashboard_downloader script conventions: env validation, run_id propagation, error handling, log forwarding).
- All-reports runner: `scripts/run_reports_sequential.sh` executes the sub-pipelines in the order above. Default behavior stops on the first failure; a `--continue-on-error` flag logs warnings and proceeds. Overall status should follow ok/warning/error rollup rules consistent with other pipelines.
- Data sources: reuse ingested/staged tables from TD/UC/bank pipelines; no new source scraping is expected for reports, only querying + PDF/CSV output as defined by each sub-pipeline spec.

---

## Pipeline General Development Guidelines

* Create Alembic Migrations with needed seed data
* Use existing logger in place
* Must follow & use exisitng infra in-place
  * using & creating record in pipelines table for every pipeline run
    * seed pipelines table (code='td_orders_sync', description='TD Orders Sync Pipeline')
    * seed pipelines table (code='uc_orders_sync', description='UC Orders Sync Pipeline')
    * seed pipelines table (code='bank_sync', description='Bank Sync Pipeline')
    * seed pipelines table (code='reports.daily_sales_report', description='Reports Pipeline, Daily Sales Report')
    * seed pipelines table (code='reports.month_to_date_sales', description='Reports Pipeline, Month To Date Sales')
    * seed pipelines table (code='reports.daily_order_processing', description='Reports Pipeline, Daily Order Processing')
    * seed pipelines table (code='reports.pending_deliveries', description='Reports Pipeline, Pending Deliveries')
    * seed pipelines table (code='reports.pending_leads_conversion', description='Reports Pipeline, Pending Lead Conversion')
    * seed pipelines table (code='reports.package_sale_conversion', description='Reports Pipeline, Package Sales Conversion')
    * seed pipelines table (code='reports.pending_payment_reconciliation', description='Reports Pipeline, Pending Payment Reconciliation')
  * using & creating record in pipeline_run_summaries table for every pipeline run
  * using notification_profiles & create needed profile for development of the modules in this document
    * seed notification_profiles: sample code for illustration
      ```sql
            insert into notification_profiles
            select id, 'td_orders_sync', 'TD Orders Sync', 'any', null, null, true from pipelines where code='td_orders_sync';

            insert into notification_profiles
            select id, 'uc_orders_sync', 'UC Orders Sync', 'any', null, null, true from pipelines where code='uc_orders_sync';

            insert into notification_profiles
            select id, 'bank_sync', 'Bank Sync', 'any', null, null, true from pipelines where code='bank_sync';

            insert into notification_profiles
            select id, 'reports.daily_sales_report', 'Daily Sales Report', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.month_to_date_sales', 'Month to Date Sales', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.daily_order_processing', 'Daily Order Processing', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.pending_deliveries', 'Pending Deliveries', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.pending_leads_conversion', 'Pending Leads Conversion', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.package_sale_conversion', 'Package Sales Conversion', 'any', null, null, true from pipelines where code='reports';

            insert into notification_profiles
            select id, 'reports.pending_payment_reconciliation', 'Pending Payment Reconciliation', 'any', null, null, true from pipelines where code='reports';
      ```
  * using email_template & create needed template for development of the pipelines in this document
  * using notification_recipients: For all the pipelines and enviroment notification should be sent/set to "wagid.sheikh@gmail.com"
  * Default email/notification templates for this phase (td_orders_sync, uc_orders_sync, bank_sync):
    * Codes and subjects:
      - `td_orders_sync`: subject `TD Orders Sync – {{status}}`
      - `uc_orders_sync`: subject `UC Orders Sync – {{status}}`
      - `bank_sync`: subject `Bank Sync – {{status}}`
    * Bodies (one template per pipeline; render status-specific blocks):
      - Header: include `Run ID: {{run_id}}`, `Overall Status: {{overall_status}}`, and run timestamps.
      - Per-store sections (td_orders_sync, uc_orders_sync): loop through stores with `store_code`, `status`, counts, and any `error_message`; include the deterministic filenames when available.
      - Bank section: list each processed file with `status`, row counts, and any error reason.
      - Status variants:
        - **ok:** emphasize success and counts per store/file, list output filenames.
        - **warning:** highlight mixed outcomes (some stores/files failed), include failed store codes/files with reasons, still list successes.
        - **error:** all failed; include concise error summaries and retry hints.
    * Status mapping: map `pipeline_run_summaries.status` values `ok`, `warning`, `error` directly to notification status; reuse the same template body with conditional sections per status.
    * Recipients: default to `wagid.sheikh@gmail.com` via `notification_recipients`; pipelines map to this recipient unless the environment overrides the mapping.
    * Aggregation for multi-store runs: always render per-store status blocks plus an overall rollup line; warning aggregation applies when at least one store succeeds and one fails.

### Staging uniqueness and upsert rules

- TD Orders staging (`stg_td_orders`): upsert/merge on `{store_code, order_number, order_date}`. Production `td_orders` should mirror these uniqueness semantics to keep re-runs idempotent.
- TD Sales staging (`stg_td_sales`): upsert/merge on `{store_code, order_number, payment_date}` with the same key alignment in `td_sales`.
- UC Orders staging (`stg_uc_orders`): upsert/merge on `{store_code, order_number, invoice_date}`; ensure the production table enforces the same uniqueness to avoid drift.
- Bank staging (`stg_bank`): upsert/merge on `{row_id}` and preserve that uniqueness in `bank` so repeated ingests update rather than duplicate rows.

### DB alignment prerequisites (before Playwright)

- Enforce the following unique constraints and use them as upsert keys in staging and production tables (already referenced above):
  - `stg_td_orders`: unique on `(store_code, order_number, order_date)`; production `td_orders` must enforce the same business key for idempotent re-runs.
  - `stg_td_sales`: unique on `(store_code, order_number, payment_date)`; production `td_sales` must match.
  - `stg_uc_orders`: unique on `(store_code, order_number, invoice_date)`; production `uc_orders` must match.
  - `stg_bank`: unique on `(row_id)`; production `bank` must match.
- Alembic migration expectations (to be completed before Playwright automation starts):
  - Apply required table creates/alters to reflect the schemas specified in this document, including indexes/constraints for the business keys above.
  - Seed data for pipelines, notification_profiles, notification_recipients, and email_templates per the earlier seeding guidance (pipeline codes for TD/UC/bank and reports).
  - Ensure migrations are idempotent: safe re-runs without duplicate seeds and with conditional constraint/index creation where applicable.

### Usage of "store_master" for pipeline development

* **Introduction:** *store_master* will act as primary controller of almost all of the pipeline development defined in this document. Any references in this document as detailed below means (uncless explicitly defined in that section/sub-sections):
  * CRM Username/Username: means store_master.sync_config.username
  * CRM Password/Password: means store_master.sync_config.password
  * cost_center/Cost Center: means store_master.cost_center
  * store_code/Store Code: means store_master.store_code
* **store_master** table alter requirements for making this pipeline work smoothly
  * alter store_master and add following columns, and seed/update data as described below
    * start_date date default null
    * cost_center varchar(8) default null
    * sync_orders_flag boolean default false
    * sync_bank_flag boolean default false
    * sync_config JSONB default null
    * sync_group char(2) default null
    * Update store_master as below:
      * for store_code 'A668' set cost_center='UN3668', sync_group = 'TD', start_date='01-Mar-2025', sync_orders_flag = true, sync_bank_flag = true
      * for store_code 'A817' set cost_center='KN3817', sync_group = 'TD', start_date='10-May-2025', sync_orders_flag = true, sync_bank_flag = true
      * for store_code 'UC567' set cost_center='SC3567', sync_group = 'UC', start_date='08-Feb-2025', sync_orders_flag = true, sync_bank_flag = true
      * for store_code 'UC610' set cost_center='SL1610', sync_group = 'UC', start_date='11-May-2025', sync_orders_flag = true, sync_bank_flag = true
    * for store_code UC567 set sync_config as
      ```json
          {
              "urls": {
                  "login": "https://store.ucleanlaundry.com/login",
                  "home": "https://store.ucleanlaundry.com/dashboard",
                  "orders_link": "https://store.ucleanlaundry.com/gst-report"
              },
              "login_selector": {
                  "username": "input[placeholder='Email'][type='email']",
                  "password": "input[placeholder='Password'][type='password']",
                  "submit": "button.btn-primary[type='submit']"
              },
              "username": "UC567@uclean.in",
              "password": "guerwnvej@uc#67"
          }
      ```
    * for store_code UC610 set sync_config as
      ```json
        {
            "urls": {
                "login": "https://store.ucleanlaundry.com/login",
                "home": "https://store.ucleanlaundry.com/dashboard",
                "orders_link": "https://store.ucleanlaundry.com/gst-report"
            },
            "login_selector": {
                "username": "input[placeholder='Email'][type='email']",
                "password": "input[placeholder='Password'][type='password']",
                "submit": "button.btn-primary[type='submit']"
            },
            "username": "UC610@uclean.in",
            "password": "vabfhwbf@uc#10"
        }
      ```
    * for store_code A668 set sync_config as
      ```json
        {
            "urls": {
                "login": "https://subs.quickdrycleaning.com/Login",
                "home": "https://subs.quickdrycleaning.com/a668/App/home",
                "orders_link": "https://simplifytumbledry.in/tms/orders",
                "sales_link": "https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport"
            },
            "login_selector": {
                "username": "txtUserId",
                "password": "txtPassword",
                "store_code": "txtBranchPin",
                "submit": "btnLogin"
            },
            "username": "tduttamnagar",
            "password": "123456"
        }
      ```
    * for store_code A817 set sync_config as
      ```json
        {
            "urls": {
                "login": "https://subs.quickdrycleaning.com/Login",
                "home": "https://subs.quickdrycleaning.com/a668/App/home",
                "orders_link": "https://simplifytumbledry.in/tms/orders",
                "sales_link": "https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport"
            },
            "login_selector": {
                "username": "txtUserId",
                "password": "txtPassword",
                "store_code": "txtBranchPin",
                "submit": "btnLogin"
            },
            "username": "tdkirtinagar",
            "password": "123456"
        }
      ```
  * General guidelines on how to use store_master.sync_config
    * **Login URL** → store_master.sync_config.urls.login
    * **Home URL** → store_master.sync_config.urls.home
    * **Orders Page URL** → store_master.sync_config.urls.orders_link
    * **Sales Page URL** → store_master.sync_config.urls.sales_link *[only applicable for store_master.sync_group='TD']*
    * **Username Input Selector** → store_master.sync_config.login_selector.username
    * **Password Input Selector** → store_master.sync_config.login_selector.password
    * **Store Input Selector** → store_master.sync_config.login_selector.store_code *[only applicable for store_master.sync_group='TD']*
    * **Submit Button Selector** → store_master.sync_config.login_selector.submit
    * **CRM Username** → store_master.sync_config.username
    * **CRM Password** → store_master.sync_config.password

---

## Staging Tables

### Table: stg_td_orders

This table will be used to hold downloaded `orders` excel data from td_orders_sync pipeline for `store_master.sync_group='TD'` stores. Proposed structure of this table is as below:

```sql
create table stg_td_orders (
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    cost_center         varchar(8),
    store_code          varchar(8),
    order_date	    timestamptz,
    order_number        varchar(12),
    customer_code	    varchar(12),
    customer_name	    varchar(128),
    customer_address    varchar(256),
    mobile_number       varchar(16),
    preference          varchar(128),
    due_date            timestamptz,
    last_activity       timestamptz,
    pieces              numeric(12,0),
    weight              numeric(12,2),
    gross_amount        numeric(12,2),
    discount            numeric(12,2),
    tax_amount          numeric(12,2),
    net_amount          numeric(12,2),
    advance             numeric(12,2),
    paid                numeric(12,2),
    adjustment          numeric(12,2),
    balance             numeric(12,2),
    advance_received    numeric(12,2),
    advance_used        numeric(12,2),
    booked_by           varchar(32),
    workshop_note       text,
    order_note          text,
    home_delivery       varchar(32),
    area_location       text,
    garments_inspected_by   varchar(32),
    customer_gstin      varchar(32),
    registration_source varchar(24),
    order_from_pos      varchar(32),
    package             varchar(32),
    package_type        varchar(32),
    package_name        varchar(32),
    feedback            varchar(32),
    tags                varchar(32),
    comment             text,
    primary_service     varchar(24),
    topup_service       varchar(32),
    order_status        varchar(32),
    last_payment_activity   timestamptz,
    package_payment_info    varchar(32),
    coupon_code         varchar(32),
    constraint pk_stg_td_order primary key (id)
);
```

### Table: stg_uc_orders

This table will be used to hold downloaded `orders` excel data from uc_orders_sync pipeline for `store_master.sync_group='UC'` stores. Proposed structure of this table is as below:

```sql
create table stg_uc_orders (
    id              bigserial,
    run_id          text,
    run_date        timestamptz,
    cost_center     varchar(8),
    store_code      varchar(8),
    s_no            numeric(10,0),
    order_number    varchar(12),
    invoice_number  varchar(12),
    invoice_date    timestamptz,
    customer_name   varchar(128),
    mobile_number   varchar(16) not null,
    payment_status  varchar(24),
    customer_gstin  varchar(32),
    place_of_supply varchar(32),
    net_amount      numeric(12,2),
    cgst            numeric(12,2),
    sgst            numeric(12,2),
    gross_amount    numeric(12,2),
    constraint pk_stg_uc_orders primary key (id)
);
```

### Table: stg_td_sales

This table will be used to hold downloaded `sales & delivery` excel data from `td_orders_sync` pipeline for `store_master.sync_group='TD'` stores. Proposed structure of this table is as below:

```sql
create table stg_td_sales (
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    cost_center         varchar(8),
    store_code          varchar(16),
    order_date          timestamptz,
    payment_date        timestamptz,
    order_number        varchar(16),
    customer_code       varchar(16),
    customer_name       varchar(128),
    customer_address    varchar(256),
    mobile_number       varchar(16),
    payment_received    numeric(12,2),
    adjustments         numeric(12,2),
    balance             numeric(12,2),
    accepted_by         varchar(64),
    payment_mode        varchar(32),
    transaction_id      varchar(64),
    payment_made_at     varchar(128),
    order_type          varchar(32),
    is_duplicate        boolean,
    is_edited_order     boolean,
    constraint pk_stg_td_sales primary key (id)
);
```

### Table stg_bank

This table will be used to hold passed (and stored in a specific folder with specific name) banks excel data from `bank_sync` pipeline. Proposed structure of this table is as below:

```sql
create table stg_bank ( 
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    bank_name           varchar(8),
    row_id              varchar(11),
    txn_date            timestamptz,
    value_date          timestamptz,
    description         varchar(256),
    ref_number          varchar(256),
    branch_code         varchar(12),
    debit               numeric(12,2),
    credit              numeric(12,2),
    balance             numeric(12,2),
    cost_center         varchar(8),
    order_number        varchar(64),
    category            varchar(16),
    sub_category        varchar(32),
    comments            varchar(256),
    constraint pk_stg_bank primary key (id),
    constraint uq_stg_bank_row_id unique (row_id)
 );
```

### Staging idempotency keys and merge rules

- Enforce unique keys in staging to guarantee idempotent upserts:
  - `stg_td_orders`: unique on (`store_code`, `order_number`, `order_date`).
  - `stg_td_sales`: unique on (`store_code`, `order_number`, `payment_date`).
  - `stg_uc_orders`: unique on (`store_code`, `order_number`, `invoice_date`).
  - `stg_bank`: unique on (`row_id`) (already noted as `uq_stg_bank_row_id`).
- Upsert/merge behavior in staging must use the above keys to avoid duplicates on re-runs for the same date ranges.
- Production-table writes must align to these same business keys to keep reruns clean:
  - `orders`: upsert TD rows on (`cost_center`, `order_number`, `order_date`) and UC rows on (`cost_center`, `order_number`, `order_date` = `invoice_date`), updating mutable fields on conflict.
  - `td_sales`: upsert on (`cost_center`, `order_number`, `payment_date`) instead of blind insert.
  - `bank`: upsert on (`row_id`) for consistency with staging.
  - Resolve conflicts by updating non-key attributes; do not insert duplicates on reruns.

---

## New Tables

### Table: orders

This table will hold data from stg_td_orders and stg_uc_orders. Business rule: perform insert if (cost_center, order_number) not exists otherwise perform update

```sql
create table orders (
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    cost_center         varchar(8) not null,
    store_code          varchar(8) not null,
    source_system       varchar(12) not null,
    order_number        varchar(12) not null,
    invoice_number      varchar(12),
    order_date      timestamptz not null,
    customer_code       varchar(12),
    customer_name       varchar(128) not null,
    mobile_number       varchar(16) not null,
    customer_gstin      varchar(32),
    customer_source     varchar(24),
    package_flag        boolean not null default false,
    service_type        varchar(24),
    customer_address    text,
    pieces              numeric(12,0),
    weight              numeric(12,2),
    due_date            timestamptz not null,
    default_due_date    timestamptz not null,
    due_days_delta      numeric(10,0),
    due_date_flag       varchar(24),
    complete_processing_by  timestamptz not null,
    gross_amount        numeric(12,2),
    discount_amount     numeric(12,2),
    tax_amount          numeric(12,2),
    net_amount          numeric(12,2),
    payment_status      varchar(24),
    order_status        varchar(24),
    payment_mode        varchar(24),
    payment_date        timestamptz,
    payment_amount      numeric(12,2),
    order_edited_flag   boolean not null default false,
    system_order_status varchar(24) Default 'Active',
    google_maps_url     varchar(256),
    latitude            DOUBLE PRECISION CHECK (latitude  BETWEEN -90  AND 90),
    longitude           DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    created_by          bigserial,
    created_at          timestamptz not null,
    updated_by          bigserial,
    updated_at          timestamptz,
    constraint pk_orders primary key (id),
    constraint uq_orders_unique_order_number unique (cost_center, order_number, order_date)
);
```

### Table: bank

This table will hold data from stg_bank. Business rule: if row_id exists then perform update, otherwise perform insert

```sql
create table bank ( 
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    bank_name           varchar(8),
    row_id              varchar(11),
    txn_date            timestamptz,
    value_date          timestamptz,
    description         varchar(256),
    ref_number          varchar(256),
    branch_code         varchar(12),
    debit               numeric(12,2),
    credit              numeric(12,2),
    balance             numeric(12,2),
    cost_center         varchar(8),
    order_number        varchar(64),
    category            varchar(16),
    sub_category        varchar(32),
    comments            varchar(256),
    constraint pk_bank primary key (id),
    constraint uq_bank_rowid unique (row_id)
 );
```

### Table: td_sales

This table will be used to hold stg_td_sales data from td_orders_sync pipeline. Always perform insert from std_td_sales to td_sales, no update. Proposed structure of this table is as below:

```sql
create table td_sales (
    id                  bigserial,
    run_id              text,
    run_date            timestamptz,
    cost_center         varchar(8),
    store_code          varchar(16),
    order_date          timestamptz,
    payment_date        timestamptz,
    order_number        varchar(16),
    customer_code       varchar(16),
    customer_name       varchar(128),
    customer_address    varchar(256),
    mobile_number       varchar(16),
    payment_received    numeric(12,2),
    adjustments         numeric(12,2),
    balance             numeric(12,2),
    accepted_by         varchar(64),
    payment_mode        varchar(32),
    transaction_id      varchar(64),
    payment_made_at     varchar(128),
    order_type          varchar(32),
    is_duplicate        boolean,
    is_edited_order     boolean,
    constraint pk_td_sales primary key (id),
    constraint unq_td_sales(cost_center, order_number, payment_date)
);
```

---

## Mapping

### from TD `order-report.xlsx` to `stg_td_orders` mapping

#### Expected Columns in order-report.xlsx

Following are expected columns to be present in excel file `order-report.xlsx` downloaded from the crm, ignore other columns if they are present.

```text
    Order Date / Time
    Order No.
    Customer Code
    Name
    Address
    Phone
    Preference
    Due Date
    Last Activity
    Pcs.
    Weight
    Gross Amount
    Discount
    Tax
    Net Amount
    Advance
    Paid
    Adjustment
    Balance
    Advance Received
    Advance Used
    Booked By
    Workshop Note
    Order Note
    Home Delivery
    Area Location
    Garments Inspected By
    Customer GSTIN
    Registration Source
    Order From POS
    Package
    Package Type
    Package Name
    Feedback
    Tags
    Comment
    Primary Services
    Top Up/Extra Service
    Order Status
    Last Payment Activity
    Package Payment Info
    Coupon Code
```

#### TD `order-report.xlsx` → `stg_td_orders` mapping

| #   | TD Excel Column       | Target column                         | Notes / Transform                                                            |
| --- | --------------------- | ------------------------------------- | ---------------------------------------------------------------------------- |
| 1   | Order Date / Time     | `stg_td_orders.order_date`            | `'Order Date / Time' → stg_td_orders.order_date`                             |
| 2   | Order No.             | `stg_td_orders.order_number`          | `'Order No.' → stg_td_orders.order_number`                                   |
| 3   | Customer Code         | `stg_td_orders.customer_code`         | `'Customer Code' → stg_td_orders.customer_code`                              |
| 4   | Name                  | `stg_td_orders.customer_name`         | `'Name' → stg_td_orders.customer_name`                                       |
| 5   | Address               | `stg_td_orders.customer_address`      | `'Address' → stg_td_orders.customer_address`                                 |
| 6   | Phone                 | `stg_td_orders.mobile_number`         | `'Phone' → stg_td_orders.mobile_number`                                      |
| 7   | Preference            | `stg_td_orders.preference`            | `'Preference' → stg_td_orders.preference`                                    |
| 8   | Due Date              | `stg_td_orders.due_date`              | `'Due Date' → stg_td_orders.due_date`; **if null** use `order_date + 3 days` |
| 9   | Last Activity         | `stg_td_orders.last_activity`         | `'Last Activity' → stg_td_orders.last_activity`                              |
| 10  | Pcs.                  | `stg_td_orders.pieces`                | `'Pcs.' → stg_td_orders.pieces`                                              |
| 11  | Weight                | `stg_td_orders.weight`                | `'Weight' → stg_td_orders.weight`                                            |
| 12  | Gross Amount          | `stg_td_orders.gross_amount`          | `'Gross Amount' → stg_td_orders.gross_amount`                                |
| 13  | Discount              | `stg_td_orders.discount`              | `'Discount' → stg_td_orders.discount`                                        |
| 14  | Tax                   | `stg_td_orders.tax_amount`            | `'Tax' → stg_td_orders.tax_amount`                                           |
| 15  | Net Amount            | `stg_td_orders.net_amount`            | `'Net Amount' → stg_td_orders.net_amount`                                    |
| 16  | Advance               | `stg_td_orders.advance`               | `'Advance' → stg_td_orders.advance`                                          |
| 17  | Paid                  | `stg_td_orders.paid`                  | `'Paid' → stg_td_orders.paid`                                                |
| 18  | Adjustment            | `stg_td_orders.adjustment`            | `'Adjustment' → stg_td_orders.adjustment`                                    |
| 19  | Balance               | `stg_td_orders.balance`               | `'Balance' → stg_td_orders.balance`                                          |
| 20  | Advance Received      | `stg_td_orders.advance_received`      | `'Advance Received' → stg_td_orders.advance_received`                        |
| 21  | Advance Used          | `stg_td_orders.advance_used`          | `'Advance Used' → stg_td_orders.advance_used`                                |
| 22  | Booked By             | `stg_td_orders.booked_by`             | `'Booked By' → stg_td_orders.booked_by`                                      |
| 23  | Workshop Note         | `stg_td_orders.workshop_note`         | `'Workshop Note' → stg_td_orders.workshop_note`                              |
| 24  | Order Note            | `stg_td_orders.order_note`            | `'Order Note' → stg_td_orders.order_note`                                    |
| 25  | Home Delivery         | `stg_td_orders.home_delivery`         | `'Home Delivery' → stg_td_orders.home_delivery`                              |
| 26  | Area Location         | `stg_td_orders.area_location`         | `'Area Location' → stg_td_orders.area_location`                              |
| 27  | Garments Inspected By | `stg_td_orders.garments_inspected_by` | `'Garments Inspected By' → stg_td_orders.garments_inspected_by`              |
| 28  | Customer GSTIN        | `stg_td_orders.customer_gstin`        | `'Customer GSTIN' → stg_td_orders.customer_gstin`                            |
| 29  | Registration Source   | `stg_td_orders.registration_source`   | `'Registration Source' → stg_td_orders.registration_source`                  |
| 30  | Order From POS        | `stg_td_orders.order_from_pos`        | `'Order From POS' → stg_td_orders.order_from_pos`                            |
| 31  | Package               | `stg_td_orders.package`               | `'Package' → stg_td_orders.package`                                          |
| 32  | Package Type          | `stg_td_orders.package_type`          | `'Package Type' → stg_td_orders.package_type`                                |
| 33  | Package Name          | `stg_td_orders.package_name`          | `'Package Name' → stg_td_orders.package_name`                                |
| 34  | Feedback              | `stg_td_orders.feedback`              | `'Feedback' → stg_td_orders.feedback`                                        |
| 35  | Tags                  | `stg_td_orders.tags`                  | `'Tags' → stg_td_orders.tags`                                                |
| 36  | Comment               | `stg_td_orders.comment`               | `'Comment' → stg_td_orders.comment`                                          |
| 37  | Primary Services      | `stg_td_orders.primary_service`       | `'Primary Services' → stg_td_orders.primary_service`                         |
| 38  | Top Up/Extra Service  | `stg_td_orders.topup_service`         | `'Top Up/Extra Service' → stg_td_orders.topup_service`                       |
| 39  | Order Status          | `stg_td_orders.order_status`          | `'Order Status' → stg_td_orders.order_status`                                |
| 40  | Last Payment Activity | `stg_td_orders.last_payment_activity` | `'Last Payment Activity' → stg_td_orders.last_payment_activity`              |
| 41  | Package Payment Info  | `stg_td_orders.package_payment_info`  | `'Package Payment Info' → stg_td_orders.package_payment_info`                |
| 42  | Coupon Code           | `stg_td_orders.coupon_code`           | `'Coupon Code' → stg_td_orders.coupon_code`                                  |

Additional columns populated **outside Excel**:

| Target column               | Notes                                                |
| --------------------------- | ---------------------------------------------------- |
| `stg_td_orders.cost_center` | Not from Excel – set from `store_master.cost_center` |
| `stg_td_orders.store_code`  | Not from Excel – set from `store_master.store_code`  |
| `stg_td_orders.run_id`      | Not from Excel – set from pipeline `run_id`          |
| `stg_td_orders.run_date`    | Not from Excel – set from pipeline `run_date`        |
| `stg_td_orders.id`          | Primary key – generated by DB                        |

##### Data Validation & Transformation Rules

These rules apply during ingestion of `order-report.xlsx` into staging table `stg_td_orders`.

| Rule # | Field / Concern                                          | Validation / Transformation Logic                                                                                    |
| -----: | -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
|     V1 | Missing `Due Date`                                       | If `Due Date` is blank → set to `order_date + interval '3 day'`                                                      |
|     V2 | Date/Time columns                                        | Convert using timezone `Asia/Kolkata`; reject rows where parsing fails                                               |
|     V3 | Numeric columns (`pieces`, `gross_amount`, `paid`, etc.) | Convert to numeric. Strip commas (,), allow .00 decimal suffix. If value not parseable → set to 0 and log a warning. |
|     V4 | `Phone` column                                           | Strip spaces,`+91`, hyphens; keep only digits; accept **10-digit** numbers only                                      |
|    V10 | Blank `Order No.`                                        | Reject row (critical key for idempotency)                                                                            |
|    V11 | Extra Columns in CRM export                              | Ignore silently (future CRM expansion safe)                                                                          |
|    V12 | Columns removed / renamed in CRM                         | Fail fast with schema-mismatch error → notify TSV-RSM                                                                |

#### from `stg_td_orders` to `orders`

| #   | Target column                   | Source / Expression                           | Notes / Transform                                                            |
| --- | ------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------- |
| 1   | `orders.id`                     | *(none)*                                      | Primary key – generated by DB                                                |
| 2   | `orders.cost_center`            | `stg_td_orders.cost_center`                   | `orders.cost_center ← stg_td_orders.cost_center`                             |
| 3   | `orders.source_system`          | `'TumbleDry'`                                 | Constant – set to `'TumbleDry'` for all rows                                 |
| 4   | `orders.store_code`             | `stg_td_orders.store_code`                    | `orders.store_code ← stg_td_orders.store_code`                               |
| 5   | `orders.order_number`           | `stg_td_orders.order_number`                  | `orders.order_number ← stg_td_orders.order_number`                           |
| 6   | `orders.invoice_number`         | `NULL`                                        | Does not come from staging – set to `NULL`                                   |
| 7   | `orders.order_date`             | `stg_td_orders.order_date`                    | `orders.order_date ← stg_td_orders.order_date`                               |
| 8   | `orders.customer_code`          | `stg_td_orders.customer_code`                 | `orders.customer_code ← stg_td_orders.customer_code`                         |
| 9   | `orders.customer_name`          | `stg_td_orders.customer_name`                 | `orders.customer_name ← stg_td_orders.customer_name`                         |
| 10  | `orders.mobile_number`          | `stg_td_orders.mobile_number`                 | `orders.mobile_number ← stg_td_orders.mobile_number`                         |
| 11  | `orders.customer_gstin`         | `stg_td_orders.customer_gstin`                | `orders.customer_gstin ← stg_td_orders.customer_gstin`                       |
| 12  | `orders.customer_source`        | `stg_td_orders.registration_source`           | `orders.customer_source ← stg_td_orders.registration_source`                 |
| 13  | `orders.package_flag`           | `stg_td_orders.package`                       | If `package = 'No'` → `FALSE`, else `TRUE`                                   |
| 14  | `orders.service_type`           | `stg_td_orders.primary_service`               | `orders.service_type ← stg_td_orders.primary_service`                        |
| 15  | `orders.customer_address`       | `stg_td_orders.customer_address`              | `orders.customer_address ← stg_td_orders.customer_address`                   |
| 16  | `orders.pieces`                 | `stg_td_orders.pieces`                        | `orders.pieces ← stg_td_orders.pieces`                                       |
| 17  | `orders.weight`                 | `stg_td_orders.weight`                        | `orders.weight ← stg_td_orders.weight`                                       |
| 18  | `orders.due_date`               | `stg_td_orders.due_date`                      | `orders.due_date ← stg_td_orders.due_date`                                   |
| 19  | `orders.default_due_date`       | `stg_td_orders.order_date + interval '3 day'` | Derived – default SLA due date                                               |
| 20  | `orders.due_days_delta`         | `orders.due_date - orders.default_due_date`   | Does not come from staging, set as `due_date` - `default_due_date`           |
| 21  | `orders.due_date_flag`          | based on `due_days_delta`                     | `0` → “Normal Delivery”; `> 0` → “Date Extended”; `< 0` → “Express Delivery” |
| 22  | `orders.complete_processing_by` | `orders.default_due_date - interval '1 day'`  | Derived – internal processing SLA                                            |
| 23  | `orders.gross_amount`           | `stg_td_orders.gross_amount`                  | `orders.gross_amount ← stg_td_orders.gross_amount`                           |
| 24  | `orders.discount_amount`        | `stg_td_orders.discount`                      | `orders.discount_amount ← stg_td_orders.discount`                            |
| 25  | `orders.tax_amount`             | `stg_td_orders.tax_amount`                    | `orders.tax_amount ← stg_td_orders.tax_amount`                               |
| 26  | `orders.net_amount`             | `stg_td_orders.net_amount`                    | `orders.net_amount ← stg_td_orders.net_amount`                               |
| 27  | `orders.payment_status`         | `'Pending'`                                   | Constant – initial value `"Pending"`                                         |
| 28  | `orders.order_status`           | `'Pending'`                                   | Constant – initial value `"Pending"`                                         |
| 29  | `orders.payment_mode`           | `NULL`                                        | Not set at this stage                                                        |
| 30  | `orders.payment_date`           | `NULL`                                        | Not set at this stage                                                        |
| 31  | `orders.payment_amount`         | `NULL`                                        | Not set at this stage                                                        |
| 32  | `orders.order_edited_flag`      | `FALSE`                                       | Default edit flag                                                            |
| 33  | `orders.system_order_status`    | `'Active'`                                    | System-level status                                                          |
| 34  | `orders.created_by`             | `1`                                           | System user id used for ETL                                                  |
| 35  | `orders.created_at`             | pipeline `run_date`                           | Set to ETL run date                                                          |
| 36  | `orders.updated_by`             | `NULL`                                        | Not set at initial load                                                      |
| 37  | `orders.updated_at`             | `NULL`                                        | Not set at initial load                                                      |
| 38  | `orders.run_id`                 | pipeline `run_id`                             | Traceability of ETL run                                                      |
| 39  | `orders.run_date`               | pipeline `run_date`                           | Same as created_at / ETL execution timestamp                                 |

### from UC `GST_Report_2025-12-01_to_2025-12-01.xlsx` to `stg_uc_orders` mapping

#### Expected Columns in GST_Report_2025-12-01_to_2025-12-01.xlsx

```text
    S.No.
    Booking ID
    Invoice No.
    Invoice Date
    Customer Name
    Customer Ph. No.
    Payment Status
    Customer GSTIN
    Place of Supply
    Taxable Value
    CGST
    SGST
    Total Invoice Value
```

#### UC `GST_Report_2025-12-01_to_2025-12-01.xlsx` → `stg_uc_orders` mapping

| #   | UC Excel Column     | Target column                   | Notes / Transform                                                         |
| --- | ------------------- | ------------------------------- | ------------------------------------------------------------------------- |
| 1   | S.No.               | `stg_uc_orders.s_no`            | `'S.No.' → stg_uc_orders.s_no`                                            |
| 2   | Booking ID          | `stg_uc_orders.order_number`    | `'Booking ID' → stg_uc_orders.order_number`                               |
| 3   | Invoice No.         | `stg_uc_orders.invoice_number`  | `'Invoice No.' → stg_uc_orders.invoice_number`                            |
| 4   | Invoice Date        | `stg_uc_orders.invoice_date`    | `'Invoice Date' → stg_uc_orders.invoice_date`                             |
| 5   | Customer Name       | `stg_uc_orders.customer_name`   | `'Customer Name' → stg_uc_orders.customer_name`                           |
| 6   | Customer Ph. No.    | `stg_uc_orders.mobile_number`   | `'Customer Ph. No.' → stg_uc_orders.mobile_number`; must be **10 digits** |
| 7   | Payment Status      | `stg_uc_orders.payment_status`  | `'Payment Status' → stg_uc_orders.payment_status`                         |
| 8   | Customer GSTIN      | `stg_uc_orders.customer_gstin`  | `'Customer GSTIN' → stg_uc_orders.customer_gstin`                         |
| 9   | Place of Supply     | `stg_uc_orders.place_of_supply` | `'Place of Supply' → stg_uc_orders.place_of_supply`                       |
| 10  | Taxable Value       | `stg_uc_orders.net_amount`      | `'Taxable Value' → stg_uc_orders.net_amount`                              |
| 11  | CGST                | `stg_uc_orders.cgst`            | `'CGST' → stg_uc_orders.cgst`                                             |
| 12  | SGST                | `stg_uc_orders.sgst`            | `'SGST' → stg_uc_orders.sgst`                                             |
| 13  | Total Invoice Value | `stg_uc_orders.gross_amount`    | `'Total Invoice Value' → stg_uc_orders.gross_amount`                      |

Additional columns populated **outside Excel**:

| Target column               | Notes                                                |
| --------------------------- | ---------------------------------------------------- |
| `stg_uc_orders.cost_center` | Not from Excel – set from `store_master.cost_center` |
| `stg_uc_orders.store_code`  | Not from Excel – set from `store_master.store_code`  |
| `stg_uc_orders.run_id`      | Not from Excel – set from pipeline `run_id`          |
| `stg_uc_orders.run_date`    | Not from Excel – set from pipeline `run_date`        |
| `stg_uc_orders.id`          | Primary key – generated by DB                        |

##### Data Validation & Transformation Rules

These rules apply during ingestion of `GST_Report_2025-12-01_to_2025-12-01.xlsx` into staging table `stg_uc_orders`.

| Rule# | Field / Concern                                                | Validation / Transformation Logic                                                                                                          |
| ----: | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| V3-UC | Numeric columns (`net_amount`, `cgst`, `sgst`, `gross_amount`) | Convert to numeric. Strip thousand separators (`,`), allow `.00` decimal suffix. If value cannot be parsed → set to `0` and log a warning. |
| V4-UC | `Customer Ph. No.`                                             | Strip spaces,`+91`, hyphens; retain only digits. Phone must be **10 digits**, otherwise set to `NULL` and log warning.                     |

#### from `stg_uc_orders` to `orders`

| #   | Target column                   | Source / Expression                             | Notes / Transform                                                            |
| --- | ------------------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------- |
| 1   | `orders.id`                     | *(none)*                                        | Primary key – generated by DB                                                |
| 2   | `orders.cost_center`            | `stg_uc_orders.cost_center`                     | `orders.cost_center ← stg_uc_orders.cost_center`                             |
| 3   | `orders.source_system`          | `'UClean'`                                      | Constant – set to `'UClean'` for all UC rows                                 |
| 4   | `orders.store_code`             | `stg_uc_orders.store_code`                      | `orders.store_code ← stg_uc_orders.store_code`                               |
| 5   | `orders.order_number`           | `stg_uc_orders.order_number`                    | `orders.order_number ← stg_uc_orders.order_number`                           |
| 6   | `orders.invoice_number`         | `stg_uc_orders.invoice_number`                  | `orders.invoice_number ← stg_uc_orders.invoice_number`                       |
| 7   | `orders.order_date`             | `stg_uc_orders.invoice_date`                    | Parsed as timestamptz                                                        |
| 8   | `orders.customer_code`          | `NULL`                                          | No customer code in staging table                                            |
| 9   | `orders.customer_name`          | `stg_uc_orders.customer_name`                   | `orders.customer_name ← stg_uc_orders.customer_name`                         |
| 10  | `orders.mobile_number`          | `stg_uc_orders.mobile_number`                   | Normalize to 10-digit mobile                                                 |
| 11  | `orders.customer_gstin`         | `stg_uc_orders.customer_gstin`                  | `orders.customer_gstin ← stg_uc_orders.customer_gstin`                       |
| 12  | `orders.customer_source`        | `'Walk in Customer'`                            | Constant                                                                     |
| 13  | `orders.package_flag`           | `FALSE`                                         | No package concept in staging table                                          |
| 14  | `orders.service_type`           | `'UNKNOWN'`                                     | Constant; can be refined later                                               |
| 15  | `orders.customer_address`       | `NULL`                                          | Address not present in staging table                                         |
| 16  | `orders.pieces`                 | `0`                                             | Not available; default to 0                                                  |
| 17  | `orders.weight`                 | `0`                                             | Not available; default to 0                                                  |
| 18  | `orders.due_date`               | `stg_uc_orders.invoice_date + interval '3 day'` | `stg_uc_orders.invoice_date + interval '3 day'`                              |
| 19  | `orders.default_due_date`       | `stg_uc_orders.invoice_date + interval '3 day'` | `stg_uc_orders.invoice_date + interval '3 day'`                              |
| 20  | `orders.due_days_delta`         | `orders.due_date - orders.default_due_date`     | Does not come from staging; computed in ETL                                  |
| 21  | `orders.due_date_flag`          | based on `due_days_delta`                       | `0` → “Normal Delivery”; `> 0` → “Date Extended”; `< 0` → “Express Delivery” |
| 22  | `orders.complete_processing_by` | `orders.default_due_date - interval '1 day'`    | Derived – internal processing SLA                                            |
| 23  | `orders.gross_amount`           | `stg_uc_orders.gross_amount`                    | `stg_uc_orders.gross_amount`                                                 |
| 24  | `orders.discount_amount`        | `0`                                             | No discount column in staging table                                          |
| 25  | `orders.tax_amount`             | `stg_uc_orders.cgst + stg_uc_orders.sgst`       | Sum of CGST and SGST                                                         |
| 26  | `orders.net_amount`             | `stg_uc_orders.net_amount`                      | UC “Taxable Value”                                                           |
| 27  | `orders.payment_status`         | `'Pending'`                                     | Constant – initial value `"Pending"`                                         |
| 28  | `orders.order_status`           | `'Pending'`                                     | Constant – initial value `"Pending"`                                         |
| 29  | `orders.payment_mode`           | `NULL`                                          | Not set at this stage                                                        |
| 30  | `orders.payment_date`           | `NULL`                                          | Not set at this stage                                                        |
| 31  | `orders.payment_amount`         | `NULL`                                          | Not set at this stage                                                        |
| 32  | `orders.order_edited_flag`      | `FALSE`                                         | Default edit flag                                                            |
| 33  | `orders.system_order_status`    | `'Active'`                                      | System-level status                                                          |
| 34  | `orders.google_maps_url`        | `NULL`                                          | Not populated from staging table                                             |
| 35  | `orders.latitude`               | `NULL`                                          | Not populated from staging table                                             |
| 36  | `orders.longitude`              | `NULL`                                          | Not populated from staging table                                             |
| 37  | `orders.created_by`             | `1`                                             | System user id used for ETL                                                  |
| 38  | `orders.created_at`             | `stg_uc_orders.run_date`                        | Set to ETL run date                                                          |
| 39  | `orders.updated_by`             | `NULL`                                          | Not set at initial load                                                      |
| 40  | `orders.updated_at`             | `NULL`                                          | Not set at initial load                                                      |
| 41  | `orders.run_id`                 | `stg_uc_orders.run_id`                          | Traceability of ETL run                                                      |
| 42  | `orders.run_date`               | `stg_uc_orders.run_date`                        | Same as created_at / ETL execution timestamp                                 |

### from `1170-nQ2-KWQLOo_udNoF6SNoa.xlsx` Sales & Delivery to `stg_td_sales`

#### Expected Columns in Downloaded `1170-nQ2-KWQLOo_udNoF6SNoa.xlsx`

Following are expected columns to be present in excel file `1170-nQ2-KWQLOo_udNoF6SNoa.xlsx` downloaded from the crm, ignore other columns if they are present.

```text
Order Date
Payment Date
Order Number
Customer Code
Customer Name
Customer Address
Customer Mobile No.
Payment Received
Adjustments
Balance
Accept By
Payment Mode
Online TransactionID
Payment Made At
Type
```

#### Excel Sales & Delivery `1170-nQ2-KWQLOo_udNoF6SNoa.xlsx` to → `stg_td_sales` mapping

| #   | Excel Column Name    | Table Name   | Column Name      | Notes / Transform                                                                                    |
| --- | -------------------- | ------------ | ---------------- | ---------------------------------------------------------------------------------------------------- |
| 1   | Order Date           | stg_td_sales | order_date       | Order Date →`stg_td_sales.order_date` • Parse date • Excel format: `DD MMM YYYY`                     |
| 2   | Payment Date         | stg_td_sales | payment_date     | Payment Date →`stg_td_sales.payment_date` • Parse datetime • Excel format: `DD MMM YYYY HH:MM:SS AM` |
| 3   | Order Number         | stg_td_sales | order_number     | Order Number →`stg_td_sales.order_number`                                                            |
| 4   | Customer Code        | stg_td_sales | customer_code    | Customer Code →`stg_td_sales.customer_code`                                                          |
| 5   | Customer Name        | stg_td_sales | customer_name    | Customer Name →`stg_td_sales.customer_name`                                                          |
| 6   | Customer Address     | stg_td_sales | customer_address | Customer Address →`stg_td_sales.customer_address`                                                    |
| 7   | Customer Mobile No.  | stg_td_sales | mobile_number    | Customer Mobile No. →`stg_td_sales.mobile_number`                                                    |
| 8   | Payment Received     | stg_td_sales | payment_received | Payment Received →`stg_td_sales.payment_received`                                                    |
| 9   | Adjustments          | stg_td_sales | adjustments      | Adjustments →`stg_td_sales.adjustments`                                                              |
| 10  | Balance              | stg_td_sales | balance          | Balance →`stg_td_sales.balance`                                                                      |
| 11  | Accept By            | stg_td_sales | accepted_by      | Accept By →`stg_td_sales.accepted_by`                                                                |
| 12  | Payment Mode         | stg_td_sales | payment_mode     | Payment Mode →`stg_td_sales.payment_mode`                                                            |
| 13  | Online TransactionID | stg_td_sales | transaction_id   | Online TransactionID →`stg_td_sales.transaction_id`                                                  |
| 14  | Payment Made At      | stg_td_sales | payment_made_at  | Payment Made At →`stg_td_sales.payment_made_at`                                                      |
| 15  | Type                 | stg_td_sales | order_type       | Type →`stg_td_sales.order_type`                                                                      |
| 16  | *(Not in Excel)*     | stg_td_sales | store_code       | Set from `store_master.store_code`                                                                   |
| 17  | *(Not in Excel)*     | stg_td_sales | cost_center      | Set from `store_master.cost_center`                                                                  |
| 18  | *(Not in Excel)*     | stg_td_sales | is_duplicate     | After ingest → if duplicate (`store_code`, `order_number`) then `TRUE` else `FALSE`                  |
| 19  | *(Not in Excel)*     | stg_td_sales | is_edited_order  | After ingest → if duplicate (`store_code`, `order_number`) then `TRUE` else `FALSE`                  |
| 20  | *(Not in Excel)*     | stg_td_sales | run_id           | Set from ETL `RUN_ID`                                                                                |
| 21  | *(Not in Excel)*     | stg_td_sales | run_date         | Set from ETL `RUN_DATE`                                                                              |
| 22  | *(Not in Excel)*     | stg_td_sales | created_at       | Set from ETL `RUN_DATE`                                                                              |

#### from `stg_td_sales` to `td_sales`

The table structure of `stg_td_sales` and `td_sales` are almost identical and perform append from `stg_td_sales` to `td_sales`

### from `*bank.xlsx` to `stg_bank

Excel File *bank.xlsx will be available in app/bank_sync/data, if the file is available it is picked and ETL performed as per mapping below.

#### Expected Columns in `*bank.xlsx`

Following are expected columns to be present in excel file `*bank.xlsx` app/bank_sync/data, ignore other columns if they are present.

```text
Bank
ID
Txn Date
Value Date
Description
Ref No./Cheque No.
Branch Code
Debit
Credit
Balance
Comments
Cost Center
Order ID
Category
Sub Category
```

#### Excel Bank Data `*bank.xlsx` to → `stg_bank` mapping

#### Bank Excel → `stg_bank` (Mapping Specification)

| #   | Bank Excel Column  | Table Name | Column Name  | Notes / Transform                         |
| --- | ------------------ | ---------- | ------------ | ----------------------------------------- |
| 1   | Bank               | stg_bank   | bank_name    | Bank →`stg_bank.bank_name`                |
| 2   | ID                 | stg_bank   | row_id       | ID →`stg_bank.row_id`                     |
| 3   | Txn Date           | stg_bank   | txn_date     | Txn Date →`stg_bank.txn_date`             |
| 4   | Value Date         | stg_bank   | value_date   | Value Date →`stg_bank.value_date`         |
| 5   | Description        | stg_bank   | description  | Description →`stg_bank.description`       |
| 6   | Ref No./Cheque No. | stg_bank   | ref_number   | Ref No./Cheque No. →`stg_bank.ref_number` |
| 7   | Branch Code        | stg_bank   | branch_code  | Branch Code →`stg_bank.branch_code`       |
| 8   | Debit              | stg_bank   | debit        | Debit →`stg_bank.debit`                   |
| 9   | Credit             | stg_bank   | credit       | Credit →`stg_bank.credit`                 |
| 10  | Balance            | stg_bank   | balance      | Balance →`stg_bank.balance`               |
| 11  | Comments           | stg_bank   | comments     | Comments →`stg_bank.comments`             |
| 12  | Cost Center        | stg_bank   | cost_center  | Cost Center →`stg_bank.cost_center`       |
| 13  | Order ID           | stg_bank   | order_number | Order ID →`stg_bank.order_number`         |
| 14  | Category           | stg_bank   | category     | Category →`stg_bank.category`             |
| 15  | Sub Category       | stg_bank   | sub_category | Sub Category →`stg_bank.sub_category`     |
| 16  | *(Not in Excel)*   | stg_bank   | id           | Primary key – generated by DB             |
| 17  | *(Not in Excel)*   | stg_bank   | run_id       | Set from ETL `RUN_ID`                     |
| 18  | *(Not in Excel)*   | stg_bank   | run_date     | Set from ETL `RUN_DATE`                   |

#### `stg_bank` → `bank`

The table structure of `stg_bank` and `bank` are almost identical. If stg_bank.row_id does not exist in `bank` perform insert else perform update for `bank.comments`, `bank.cost_center`, `bank.order_number`, `bank.category`, `bank.sub_category`
----------------------------------------------------------------------------------------------------------------------------------------

## Playwright Orchestration

### Phase 1 Playwright scope (after DB alignment)

- First executable slice: **TD Orders** only — perform login with session probe, enter iframe, and wait for hydration. No date selection or download in this increment.
  - Steps: launch per-store context with storage_state probe → navigate to home with store_code in URL → open Reports → Orders container → enter iframe `#ifrmReport` → wait for iframe `src` to be non-empty → wait for hydration cues (spinner gone or primary controls visible).
  - Outputs to capture during runs:
    - Observed selectors/roles inside the iframe (buttons/links like Expand, Download Historical Report, Generate Report).
    - Spinner/loader cues and how they disappear (classes, aria labels, or text).
    - Iframe DOM details: final `src`, presence of nested frames if any, and any accessibility roles that worked for locators.
- TD Sales and UC flows are **on hold** until this slice is validated; keep their selectors unchanged for now.

## Pipeline td_orders_sync development guidelines

*Introduction* this pipeline is supposed to download orders and sales data for TD group stores (store_master.sync_group = 'TD'). Two Excel files will be downloaded & ingested as per mapping already given above. Pull records from store_master where sync_group = 'TD' and sync_orders_flag = true (there will be multiple rows), For each store_master row, open a fresh browser context, perform login, navigate to home, then download Orders and Sales & Delivery data before closing the context.

#### Session reuse, expiry detection, and retry order

- Use per-store storage state files under `app/crm_downloader/profiles` named `{store_code}_storage_state.json`.
- Probe order (per store):
  1. Launch Playwright context with `storageState` pointing to the store file if it exists.
  2. Navigate to `store_master.sync_config.urls.home` to verify session validity.
  3. Session-expiry detection signals: redirect to login URL, appearance of login form selectors, or absence of logged-in controls (e.g., Reports/Order links). Any of these must trigger a re-login using the same store’s credentials.
- On expiry: perform full login with credentials + store code, regenerate a fresh context, and overwrite the same `{store_code}_storage_state.json`. Retry the navigation + download flow once after refresh; on persistent failure mark the store run as failed. Keep expiry handling store-isolated—do not let one store’s failure delete or reuse another store’s cookies, and refresh storage_state only for the affected store.
- Do not cross-store state: never reuse one store’s storage state for another store.
- URLs after login auto-embed `{store_code}`; do **not** hardcode `a668` or any specific code—always derive from `store_master.store_code` and verify post-login URLs contain that value.

#### TD Playwright iframe entry and hydration readiness

- Iframe entry: always target `frameLocator('#ifrmReport')` (preferred) or `contentFrame()` once attached. Wait for the iframe to be attached with a non-empty `src` (typically `reports.quickdrycleaning.com/...`).
- Hydration waits: inside the iframe, wait for the spinner to disappear **or** for known controls to become visible (e.g., `Expand`, `Download Historical Report`, `Generate Report`, `Request Report`). Prefer `expect(locator).to_be_visible()` over network idle waits, and do not interact until hydration completes.
- Locator preference: use role-based locators first (`getByRole("button", { name: "Generate Report" })`, `getByRole("link", { name: /Download historical report/i })`). If those fail due to custom components, fall back to text locators such as `locator("text=Download Historical Report")` or `locator("text=Expand")`.
- Post-login navigation rules: URLs embed `{store_code}` after login; validate the live URL includes the current store before proceeding and avoid any hardcoded store_code in locators or URLs.
- Session expiry handling in-iframe: if iframe content renders a login redirect, missing logged-in controls, or other expiry signals, treat it the same as parent-level expiry—refresh per-store storage_state through re-login, then retry iframe navigation once. Only the affected store’s state file should be replaced during this recovery.

### Navigating to Login & Home Page

1. Open the link given store_master.sync_config.urls.login (https://subs.quickdrycleaning.com/Login)
2. Use provided store_master.sync_config.login_selectors and fill in store_master.sync_config.username store_master.sync_config.password and store_master.store_code
3. Click Login and you will reach to home page store_master.sync_config.urls.home (the server auto-injects `/{store_master.store_code}/` into the path).
4. Verify and Note that in the home page url {store_master.store_code} is also present

### Excel File 1: Orders Data

1. On home page, the Reports tile selector (navigates to the Order Report container page) is:
   `<a id="achrOrderReport" href="Reports/OrderReport.aspx" class="padding6" onclick="return checkAccessRights('Orders')"><i class="fa fa-bar-chart-o fa-3x"></i><br /><span>&nbsp;Reports&nbsp;</span></a>`
2. Click on this 'Reports' menu option and verify navigation to a URL containing `/App/Reports/OrderReport` and `{store_master.store_code}`. After the container page loads, switch into iframe `#ifrmReport`, run the “Historical Report” workflow, and download the generated report.

- Iframe entry rule: always enter via `frameLocator('#ifrmReport')` (preferred) or `contentFrame()` once attached. Do not attempt to click iframe children from the parent page.
- Wait for iframe `#ifrmReport` to be attached and to expose a non-empty `src` (preferably containing `reports.quickdrycleaning.com`).
- Hydration/readiness: wait for the spinner to disappear and for any of the following to become visible inside the iframe: `Expand`, `Download Historical Report`, or the primary action buttons. Use `frame.getByRole(...).waitFor()` or visibility assertions rather than networkidle.
- Fallback locators: prefer role-based locators (`getByRole("button", { name: "Generate Report" })`, `getByRole("link", { name: /Download historical report/i })`). If roles are unavailable, fall back to text-based locators (`locator("text=Download Historical Report")`, `locator("text=Expand")`).

Critical architecture facts (do not miss):

1) The Orders Report UI is NOT in the parent page DOM. It is inside iframe "#ifrmReport".
2) The iframe loads a Next.js app from "https://reports.quickdrycleaning.com/{locale}/order-report?...". The initial HTML shows only a spinner; the real UI (Expand / Download Historical Report / Generate Report / date picker / Request Report / Report Requests table) appears only AFTER JS hydration. Therefore: do NOT rely on view-source HTML. Use runtime locators with explicit waits.

Iframe entry:

- Use frameLocator("#ifrmReport") (recommended) or elementHandle.contentFrame().

Hydration wait inside iframe:

- Wait for spinner to disappear OR for a known UI control to appear.
  Example: wait for "text=Expand" or "text=Download Historical Report" (whichever appears first).
- Do not use only networkidle; prefer "expect(locator).to_be_visible()".

Workflow inside iframe (in order):

1) Click the control labeled "Expand" (if not a native button, locate by visible text and click the nearest clickable ancestor).
2) Click "Download historical Report" (this triggers partial refresh/state change).
3) Wait until "Generate Report" becomes visible, then click it.
4) A date-range picker overlay appears.
   - Select from_date and to_date (passed as function parameters).
   - Implement robust date selection:
     a) If there are editable inputs, fill them directly (preferred).
     b) If inputs are readonly, interact with the calendar UI to pick dates.
5) Click "Request Report".
6) After Request Report:
   - Wait for loading animation to appear then disappear OR
   - Wait for "Report Requests" section/table to show/update and contain a row whose date range text matches the UI display format `DD Mon YYYY - DD Mon YYYY` (e.g., `01 Oct 2025 - 06 Dec 2025`) for the requested dates.
   - If duplicate rows exist for the same range, pick the newest row (top-most timestamp / latest request status) that exactly matches the text before clicking download.
7) Download:
   - Do not click Download until the row text exactly matches `DD Mon YYYY - DD Mon YYYY` for the requested window; when multiple matches exist, pick the newest/top-most entry before proceeding.
   - Wrap the exact click that triggers download in `page.expect_download()` / `page.waitForEvent('download')` to avoid race conditions, then save to disk with a deterministic filename:
     `{store_master.store_code}_td_orders_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`. Confirm whether the intended date token is `YYYYMMDD`; align the saved filename format once confirmed.
8) Locator rules:

- Prefer getByRole within the iframe:
  - frame.getByRole("button", { name: "Generate Report" })
  - frame.getByRole("button", { name: "Request Report" })
  - frame.getByRole("link", { name: /Download historical Report/i })
- If role locators fail (custom components), fallback to has-text selectors:
  - frame.locator("text=Download Historical Report")
- Never search these UI elements on the parent page.

### Excel File 2: Sales And Delivery Data

Sales & Delivery Historical Report (within iframe) – Automation Steps (Playwright, same iframe entry/hydration rules as Orders report, except there is no "Expand" step):
After login, navigate to Reports → Sales and Delivery and verify the container page URL is /{store_master.store_code}/App/Reports/NEWSalesAndDeliveryReport…. This page renders the actual report UI inside the iframe `iframe#ifrmReport` (its src is set dynamically). Rules:

- Enter the iframe with `frameLocator('#ifrmReport')` and wait for non-empty `src` + spinner disappearance or primary controls visibility.
- Prefer role-based locators (`getByRole("button", { name: "Generate Report" })`, `"Request Report"`, `"Download"`) with text fallbacks.
- Inside the iframe: click the link/button labeled “Download historical report”; wait until the “Generate Report” button becomes visible and click it; a date-range overlay opens—set From and To dates using the date picker (or fill the inputs if they exist) and click “Update” to close the overlay; then click “Request Report”.
- After requesting, wait for the spinner/loading animation to finish and for the “Report Requests” table to appear. Locate the row whose date range text exactly matches the requested UI-formatted range `DD Mon YYYY - DD Mon YYYY`; if multiple matches exist, pick the newest entry before clicking “Download” in that row only.
- Use `page.waitForEvent('download')` (scoped to the click) and save using `{store_master.store_code}_td_sales_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`. Confirm whether the intended date token is `YYYYMMDD` and apply the chosen format consistently after validation.

#### TD Orders/Sales filename and date-range matching rules

- Deterministic filenames after saving downloads:
  - Orders: `{store_code}_td_orders_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`
  - Sales: `{store_code}_td_sales_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`
- Confirm whether the CRM date tokens are 7 or 8 digits; if the UI emits `YYYYMMDD`, switch to that format consistently after validation.
- Report Requests table validation:
  - Do not click Download until a row with date text exactly matching `DD Mon YYYY - DD Mon YYYY` for the requested window is visible.
  - When multiple matching rows exist, pick the newest/top-most entry (latest timestamp/status) before triggering the download.

---

## Pipeline uc_orders_sync development guidelines

*Introduction* this pipeline is supposed to download orders and sales data for UC group stores (store_master.sync_group = 'UC'). One Excel files will be downloaded & ingested as per mapping already given above. Pull records from store_master where sync_group = 'UC' and sync_orders_flag = true (there will be multiple rows), For each store_master row, open a fresh browser context, perform login, navigate to home, then download Orders data before closing the context.

#### Session reuse and expiry checks (UC)

- Use the same storage-state convention as TD: `{store_code}_storage_state.json` per store under `app/crm_downloader/profiles`.
- Probe with the stored session by navigating to the dashboard/home; session expiry signals: redirect to login, login form present, or missing logged-in menu controls (e.g., the Reports → GST Report entry).
- On expiry, perform full login, refresh the storage_state file, and retry the GST download flow once before failing the store run.

### Navigating to Login & Home Page

1. Open the link given store_master.sync_config.urls.login (https://store.ucleanlaundry.com/login)
2. Use provided store_master.sync_config.login_selectors and fill in store_master.sync_config.username store_master.sync_config.password
3. Click Login and you will reach to home page store_master.sync_config.urls.home (https://store.ucleanlaundry.com/dashboard)
4. Verify and wait for page to load

### Excel File 1: Orders Data

- UC Playwright selector and readiness guidance:
  - Navigation: click the primary navigation link labeled **“GST Report”** (role-based locator preferred) under Reports to open the GST view; treat missing/hidden GST link as a session-expiry signal.
  - Overlay controls: headings **“Start Date”** and **“End Date”** with buttons **“Apply”** and **“Cancel”**; export action button **“Export Report”**.
  - Date display: the selected range renders as `Dec 05, 2025 - Dec 30, 2025` (format `MMM DD, YYYY - MMM DD, YYYY`).
  - Readiness cues: ensure the overlay is closed, the table has re-rendered, and any spinner/loader is gone before clicking **Export Report**. The displayed date range should match the requested window.
  - Session reuse/expiry: reuse `{store_code}_storage_state.json` per store; treat redirects to login, login form visibility, or missing logged-in controls (e.g., GST Report link absent) as expiry. Re-login, refresh the per-store storage_state, and retry once before failing.
- Confirm the application routes to https://store.ucleanlaundry.com/gst-report and the GST Report view is rendered (Angular SPA content is client-side within the app shell).
- Set the report filter range by selecting From Date and To Date in the overlay, then click Apply (or Cancel to dismiss without changes). Wait for the overlay to close and for the table/spinner to settle before continuing.
- Click Export Report to trigger an .xlsx download only after the table is refreshed for the selected range. Save the downloaded file using the naming convention:
  `{store_master.store_code}_historical_sales_report_{from-date}_to_{to-date}.xlsx`
  (where `{from-date}` and `{to-date}` are the same values used in the filters, formatted consistently for filenames, and aligned with the displayed `Dec 05, 2025 - Dec 30, 2025` text).
