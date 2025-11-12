
# **SRS: CRM Data Download Module**

### **System Context**

This module — *CRM Data Download* — extends the existing automation platform that already performs other Playwright-based data download and ingestion operations (the *Existing Data Download System*).
It adds independent logic and scheduling to download **sales data from two CRM systems** used in The Shaw Ventures’ laundry operations:

* **CRM1:** TumbleDry (subs.quickdrycleaning.com + reports.quickdrycleaning.com)
* **CRM2:** UClean (store.ucleanlaundry.com)

Each CRM has **two independent store accounts** (total **4 credentials**) that must be logged in separately and maintained as separate Playwright storage states.

---

## **1. Objectives**

The purpose of this module is to:

* Automate downloading of daily or intra-day **sales data reports** from both CRM systems.
* Maintain separate browser sessions for each store credential.
* Schedule independent downloads multiple times per day.
* Ingest the downloaded files into PostgreSQL staging tables.
* Maintain an internal audit ledger of downloads, ingestion status, and timestamps.

This module will run independently of other download modules in the system but will **reuse core system utilities**, such as:

* Environment/config management
* Logging and cron job framework
* Database and ingestion utilities
* File management utilities

---

## **2. Functional Requirements**

### **2.1 Supported Systems and Credentials**

| CRM                  | Store Name              | Store Code | Login URL                                                                        | Report Source                                                                                                | Frequency     |
| -------------------- | ----------------------- | ---------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------- |
| **TumbleDry (CRM1)** | Uttam Nagar             | a668       | [https://subs.quickdrycleaning.com/a668](https://subs.quickdrycleaning.com/a668) | [https://reports.quickdrycleaning.com/en/order-report](https://reports.quickdrycleaning.com/en/order-report) | 3–4 times/day |
| **TumbleDry (CRM1)** | Kirti Nagar             | a817       | [https://subs.quickdrycleaning.com/a817](https://subs.quickdrycleaning.com/a817) | [https://reports.quickdrycleaning.com/en/order-report](https://reports.quickdrycleaning.com/en/order-report) | 3–4 times/day |
| **UClean (CRM2)**    | Sector 56, Gurugram     | –          | [https://store.ucleanlaundry.com/login](https://store.ucleanlaundry.com/login)   | /api/v1/sales/export-report?type=GST&format=excel                                                            | 3–4 times/day |
| **UClean (CRM2)**    | Sushant Lok 1, Gurugram | –          | [https://store.ucleanlaundry.com/login](https://store.ucleanlaundry.com/login)   | /api/v1/stores/generateGST?franchise=UCLEAN                                                                  | 3–4 times/day |

Each store operates in an isolated CRM account and requires its own login and stored Playwright session.

---

### **2.2 Login & Authorization**

* Each store must be **authorized individually** (first run via headed Chrome).
* Once login is complete and OTP/device authorization is done, the Playwright session (`storage_state.json`) is saved per store:

  ```
  profiles/td_a668.json
  profiles/td_a817.json
  profiles/uc_sector56.json
  profiles/uc_sushantlok1.json
  ```
* The storage states are portable to the Linux deployment environment.

---

### **2.3 Download Flow**

#### **2.3.1 TumbleDry**

1. Launch Chrome (Playwright channel: `"chrome"`) using the stored session.
2. Visit the store-specific legacy URL:

   ```
   https://subs.quickdrycleaning.com/<storeCode>/App/Reports/OrderReport.aspx
   ```
3. Navigate to the reports app:

   ```
   https://reports.quickdrycleaning.com/en/order-report
   ```
4. Automatically or manually trigger the **Export** action (detected via `page.expect_download()`).
5. Capture and save the file as:

   ```
   TD_OrderReport_<StoreName>_<FromDate>_to_<ToDate>.xlsx
   ```
6. Close browser and persist updated cookies.

#### **2.3.2 UClean**

1. Launch Chrome with store’s session (via saved storage JSON).
2. Visit login/dashboard URL (`https://store.ucleanlaundry.com/login`).
3. Navigate or trigger API/Export action based on active tenant:

   * Direct GET: `/api/v1/sales/export-report?type=GST&format=excel`
   * or POST: `/api/v1/stores/generateGST?franchise=UCLEAN`
4. Capture download via `page.expect_download()` and save as:

   ```
   UC_GST_<StoreName>_<FromDate>_to_<ToDate>.xlsx
   ```
5. Close browser and persist updated cookies.

---

### **2.4 File Management**

* All downloads are stored in a single directory (configurable, default: `./downloads`).
* Filenames follow a canonical convention:

  ```
  <CRM>_<ReportType>_<StoreName>_<From>_to_<To>.xlsx
  ```
* Each completed file is recorded in a **download ledger table**.
* After ingestion, files are **deleted automatically** (unless retention flag is set).

---

### **2.5 PostgreSQL Ingestion**

After each download:

1. Compute file hash (MD5) and log it to `download_ledger`.
2. Parse XLSX → stage table:

   * `stg_td_order_report`
   * `stg_uc_gst_report`
3. Upsert to fact tables:

   * `fact_td_orders`
   * `fact_uc_gst`
4. Record ingestion summary in:

   ```
   ingest_audit(run_id, crm, store, from_date, to_date, inserted_rows, md5, started_at, finished_at, status)
   ```

---

### **2.6 Scheduling**

| Module                     | Schedule      | Notes                          |
| -------------------------- | ------------- | ------------------------------ |
| **Existing data download** | Once daily    | Other data, runs separately    |
| **CRM Data Download**      | 3–4 times/day | Sales data for all four stores |

* Each CRM Data Download run is isolated and stateless beyond the saved session.
* Date windows are computed dynamically based on `download_ledger` or CLI arguments.

---

### **2.7 CLI / Automation Interface**

All operations integrate into the existing system CLI or cron orchestration.

#### **Commands**

| Command                                                                         | Description                 |
| ------------------------------------------------------------------------------- | --------------------------- |
| `crm_download bootstrap --crm td --store a668`                                  | Bootstrap login for a store |
| `crm_download download --crm td --store a817 --from 2025-11-12 --to 2025-11-12` | Run single store download   |
| `crm_download download --crm uc --store all --from today --to today`            | Run for all UClean stores   |

#### **Arguments**

* `--crm` → `td` or `uc`
* `--store` → specific store code or `all`
* `--from`, `--to` → date range
* `--headless` → optional override (default from config)
* `--keep-files` → skip cleanup after ingestion

---

## **3. Non-Functional Requirements**

### **3.1 Compatibility**

* macOS (development), Ubuntu Linux (production)
* Playwright (channel `"chrome"`)
* Python ≥ 3.12

### **3.2 Performance**

* Each download completes in ≤ 60s (network dependent)
* Concurrent multi-store downloads optional via threading or cron-level parallelism

### **3.3 Reliability**

* Resilient to intermittent login prompts or expired sessions (prompts manual re-bootstrap)
* Each store operates independently — failure of one doesn’t block others

### **3.4 Logging & Monitoring**

* Leverages existing system logger
* Logs per run:

  ```
  [timestamp] [crm/store] [event] [filename or error]
  ```
* Exit codes integrate with cron failure alerts

### **3.5 Security**

* No plaintext credentials in code
* Uses `.env` or system config for usernames/passwords
* OTP handled manually on first bootstrap only

---

## **4. Deliverables**

1. **`multi_crm_downloader.py`**

   * Single module implementing bootstrap and download logic.
2. **`profiles/` Directory**

   * Contains Playwright storage state per store.
3. **Database Schema Extensions**

   * `download_ledger`
   * `ingest_audit`
   * `stg_td_order_report`, `fact_td_orders`
   * `stg_uc_gst_report`, `fact_uc_gst`
4. **Cron Definitions**

   * `crm_download_td.sh` – runs all TumbleDry stores
   * `crm_download_uc.sh` – runs all UClean stores

---

## **5. Integration Points**

| Component                  | Description                                               |
| -------------------------- | --------------------------------------------------------- |
| **Existing Config System** | Reuse env vars and file paths                             |
| **Shared DB Connector**    | Use same PostgreSQL connection                            |
| **Shared Logging**         | Follow standard logging format                            |
| **Scheduler (cron)**       | Run under same cron orchestration but at higher frequency |
| **Ingestion Layer**        | Reuse existing Excel → Postgres utilities                 |

---

## **6. Future Enhancements**

* Auto re-authentication if OTP/device expires.
* Parallel store downloads via async Playwright contexts.
* Automatic export button click using stored selectors.
* Error notification via email or webhook (failed downloads).
* Configurable retention policy for downloaded files.

---

✅ **Outcome**
This SRS defines a **modular, session-persistent Playwright automation** that integrates cleanly with your existing system while maintaining per-store isolation.
Codex should treat this as a **self-contained submodule** named `crm_data_download`, adhering to existing system architecture, env config patterns, and logging structure.

---
