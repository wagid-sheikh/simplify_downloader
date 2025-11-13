# SRS: CRM Data Download Module

### **System Context**

This module — *CRM Data Download* — extends the existing automation platform / data download system that already performs other Playwright-based data download and ingestion operations (the *Existing Data Download System*).

It adds independent logic and scheduling to download **sales data Excel reports** from two CRM systems used in The Shaw Ventures’ laundry operations:

- **TumbleDry CRM** (subs.quickdrycleaning.com + reports.quickdrycleaning.com)
- **UClean CRM** (store.ucleanlaundry.com)

Each CRM has **two independent store accounts** (total four stores).
Each store must be handled separately with its own authentication, Playwright storage state, and ingestion pipeline.

---

# **1. Objectives**

The module must:

1. Automate downloading daily/intra-day Excel sales reports from:
   - **TumbleDry Uttam Nagar**
   - **TumbleDry Kirti Nagar**
   - **UClean Sector 56**
   - **UClean Sushant Lok**
2. Maintain **separate browser sessions** per store using unique Playwright storage files.
3. Schedule downloads **3–4 times per day per store**.
4. Store all downloaded files using a **canonical filename convention**.
5. Maintain **download ledger** entries including date range, md5, store, CRM, and status.
6. Load raw data into **staging tables**:
   - `stg_td_order_report`
   - `stg_uc_gst_report`
7. Transform and merge into a **unified `orders` table** with:
   - Idempotent upsert behavior
   - Unified payment, tax, order timestamps, and status normalization
   - Unique business keys
8. Ensure **idempotency at 3 levels**:
   - File level (md5)
   - Ingestion run (ingest_audit)
   - Record level (unique business keys)

---

# **2. Functional Requirements**

## **2.1 Supported Systems and Credentials**

| CRM       | Store       | Code | Login URL                              | Report URL                                           | Frequency |
| --------- | ----------- | ---- | -------------------------------------- | ---------------------------------------------------- | --------- |
| TumbleDry | Uttam Nagar | a668 | https://subs.quickdrycleaning.com/a668 | https://reports.quickdrycleaning.com/en/order-report | 3–4/day   |
| TumbleDry | Kirti Nagar | a817 | https://subs.quickdrycleaning.com/a817 | https://reports.quickdrycleaning.com/en/order-report | 3–4/day   |
| UClean    | Sector 56   | –    | https://store.ucleanlaundry.com/login  | `/api/v1/sales/export-report?type=GST&format=excel`  | 3–4/day   |
| UClean    | Sushant Lok | –    | https://store.ucleanlaundry.com/login  | `/api/v1/stores/generateGST`                         | 3–4/day   |

Each store = separate logical source with its own session file and scheduling.

---

## **2.2 Login & Authorization**

### TumbleDry & UClean (common rules):

- First login must be done **in headed mode**, storing storage_state.json under:
  ```
  profiles/td_a668.json
  profiles/td_a817.json
  profiles/uc_sector56.json
  profiles/uc_sushantlok1.json
  ```
- These storage files must be **portable** to Linux VM.
- During normal runs:
  - **No OTP**
  - Playwright must load storage_state
  - If session expired → module must log a clear error and exit for that store only.

---

## **2.3 Download Flow**

### **2.3.1 TumbleDry**

Steps:

1. Launch Chrome with store storage state.
2. Hit:
   ```
   https://subs.quickdrycleaning.com/<storeCode>/App/Reports/OrderReport.aspx
   ```
3. Navigate to:
   ```
   https://reports.quickdrycleaning.com/en/order-report
   ```
4. Select date range.
5. Click Export → capture via `page.expect_download()`.
6. Save file using canonical name.

### **2.3.2 UClean**

Steps:

1. Launch Chrome with storage state.
2. Login using stored session.
3. Trigger export via API endpoints:
   - `GET /api/v1/sales/export-report?type=GST&format=excel`
   - OR `POST /api/v1/stores/generateGST`
4. Save file.

**Common rules:**

- One run can process multiple stores.
- One store failure must **not block** others.
- Each file = recorded in download_ledger.

---

## **2.4 File Management**

### Canonical filename:

```
<CRM>_<ReportType>_<StoreName>_<FromDate>_to_<ToDate>.xlsx
```

Examples:

```
TD_OrderReport_UttamNagar_2025-11-12_to_2025-11-12.xlsx
UC_GST_Sector56_2025-11-12_to_2025-11-12.xlsx
```

### Storage:

- Default directory: `./downloads/`
- After ingestion:
  - Delete unless `--keep-files` is used.

### `download_ledger` fields:

- crm
- store
- filename
- from_date
- to_date
- md5_hash
- downloaded_at
- status (downloaded / skipped / failed / duplicate)

---

# **2.5 PostgreSQL Ingestion & Transformation**

## **2.5.1 Audit Tables**

### `download_ledger`

Tracks every download attempt.

### `ingest_audit`

Tracks ingestion summaries:

- run_id
- crm, store
- from_date, to_date
- inserted_rows
- updated_rows
- md5_hash
- started_at, finished_at
- status
- error_message

---

## **2.5.2 Staging Tables**

### `stg_td_order_report`

- Mirrors TumbleDry Excel columns exactly (order id, invoice, datetime, gross, tax, net, payment mode, etc.)
- Adds:
  - source_system = 'TD'
  - store_code
  - cost_center
  - file_md5
  - load_batch_id

### `stg_uc_gst_report`

- Mirrors UClean Excel columns (bill id, invoice, taxable, GST, total, mode)
- Adds:
  - source_system = 'UC'
  - store_name
  - cost_center
  - file_md5
  - load_batch_id

---

## **2.5.3 Unified `orders` Table**

### **Purpose**

A single-source-of-truth fact table for sales analytics.

### **Schema (logical)**

```
orders (
  id BIGSERIAL PRIMARY KEY,
  cost_center TEXT NOT NULL,
  source_system TEXT NOT NULL,
  source_store_code TEXT,
  source_order_id TEXT NOT NULL,
  source_invoice_no TEXT,
  order_datetime TIMESTAMPTZ NOT NULL,
  order_date DATE NOT NULL,

  customer_name TEXT,
  customer_mobile TEXT,
  customer_code TEXT,

  gross_amount NUMERIC(12,2),
  discount_amount NUMERIC(12,2),
  taxable_amount NUMERIC(12,2),
  tax_amount NUMERIC(12,2),
  net_amount NUMERIC(12,2),

  payment_mode_summary JSONB,
  payment_status TEXT,
  order_status TEXT,

  source_payload JSONB,
  source_hash TEXT NOT NULL,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### **Business Key**

```
(cost_center, source_system, source_order_id)
```

### **Unique Constraint**

Ensures idempotency and no duplicates:

```
UNIQUE(cost_center, source_system, source_order_id)
```

### **Mapping Rules**

#### TumbleDry

- Order No → source_order_id
- Invoice No → source_invoice_no
- Booking/Bill Date & Time → order_datetime
- money fields map directly
- store_code → cost_center via mapping table

#### UClean

- Bill ID → source_order_id
- Bill No → source_invoice_no
- Bill Date → order_datetime
- taxable, gst, total → numeric fields
- store/franchise → cost_center via lookup

#### Manual Sources

- Similar structure but:
  - source_system = 'MANUAL'

---

## **2.5.4 Idempotent Merge Logic**

1. Load staging rows with `file_md5` & `load_batch_id`.
2. Convert to unified `orders` rows with computed:
   - cost_center
   - source_system
   - source_order_id
   - source_invoice_no
   - source_hash
3. UPSERT logic:
   - If not exists → INSERT
   - If exists but hash differs → UPDATE
   - If hash same → SKIP
4. Ingest audit updated accordingly.

---

# **2.6 Scheduling**

| Module            | Schedule          |
| ----------------- | ----------------- |
| Existing download | Daily             |
| CRM Download      | 3–4 times per day |

- Date windows computed automatically from last successful ingestion.
- Overlapping runs are allowed due to idempotency.

---

# **2.7 CLI Interface**

### Commands:

| Command                                                                         | Description       |
| ------------------------------------------------------------------------------- | ----------------- |
| `crm_download bootstrap --crm td --store a668`                                  | Bootstrap login   |
| `crm_download download --crm td --store a817 --from 2025-11-12 --to 2025-11-12` | Download & ingest |
| `crm_download download --crm uc --store all --from today --to today`            | All UC stores     |

### Parameters:

- `--crm`
- `--store`
- `--from`
- `--to`
- `--headless`
- `--keep-files`

---

# **2.8 Error Handling & Alerts**

- Failures logged with store + date context.
- Partial failures allowed.
- Missing data → NO_DATA (not a failure).
- Future: webhook/email alerts.

---

# **3. Non-Functional Requirements**

## 3.1 Compatibility

- macOS dev, Ubuntu prod
- Python 3.12+
- Playwright with `"chrome"` channel

## 3.2 Performance

- Per-store runtime < 60 sec
- Optional async parallelization

## 3.3 Reliability

- Resilient to session expiry
- One store failure does not stop others

## 3.4 Logging

Unified log format:

```
[timestamp] [level] [crm/store] [event] [filename/error]
```

## 3.5 Security

- No plaintext credentials in code
- Storage states protected
- OTP only during bootstrap

---

# **4. Deliverables**

1. `multi_crm_downloader.py`
2. Storage profiles
3. DB schema:
   - download_ledger
   - ingest_audit
   - stg_td_order_report
   - stg_uc_gst_report
   - orders
4. Cron wrappers

---

# **5. Integration Points**

- Shared config
- Shared db connector
- Shared logging
- Cron scheduler
- Common DataFrame → Postgres utilities

---

# **6. Future Enhancements**

- Auto re-auth
- Async multi-store downloads
- Webhook/email alerts
- Archival auto-cleanups
- Smart UI selector updates

---
