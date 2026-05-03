# tsv_bank_payment_sync_requirement

## User Original Prompt

This session will be a barnstorming session. A lot of questions and answers. I have a google sheet named "TSV-Bank-Statements-And-Payments" and the URL of it is "https://docs.google.com/spreadsheets/d/16nzFcGpA3zvYY0Zu6ZR5Y_OVSUJrrRfaoVUO1qHNZv4/edit?gid=904558821#gid=904558821". It contains following tabs: "IDFC-Bank", "SBI-Bank", "Payments", "Packages". I would like you to guide me to provide you all the needed information to dynamically access this Google Sheet and all it's tabs and sync data in tables. This module can be called "tsv_bank_payment_sync" pipeline just like other pipelines in the system. Any configuration needed shall be stored in system_config table once you explain to me.

Phase 1 steps:

1. Access this Google Sheet
2. Read structure and data of all tabs
3. Push those records/data into PostgreSQL tables which our system is configured for

Phase 2:

1. Identify de-dupe keys
2. Reconcile sales records with actual bank records for online payments
3. Reconcile sales records with actual cash payments
4. Find orders that are not actually paid (but marked as paid) vs actually paid
5. Identify Today's actual collections vs marked in sales table

First create a markdown file tsv_bank_payment_sync_requirement.md and include my original prompt and your questions that I can answers. I will write my answers in that markdown file, then you read and complete creation of pipeline requirements.

---

## Assistant Questions (Please answer all)

### A) Google Sheet Access & Security

1. Should this pipeline use:

   - Google Service Account (recommended for server automation), or
   - OAuth user token?
2. If Service Account:

   - Please share the service-account email you will use (or confirm you want us to generate expected config keys only).
   - Confirm the Google Sheet is shared with that service-account email as **Viewer** or **Editor**.
3. Where should credentials be stored?

   - In `system_config` as encrypted JSON string?
   - Or file path reference from env + key in `system_config`?
4. Should we support one sheet only (current URL), or multiple sheet IDs in future?
5. Confirm production behavior if sheet is temporarily inaccessible:

   - Fail the pipeline run immediately, or
   - Skip and notify, or
   - Retry with backoff then fail?

   [Answers: 1: **service account + Google Sheets API v4/gspread** . 2. Generate keys and store them our system_config table as rest of the system uses that table. 3. system_config 4. support multiple sheet (altohough this enahcement require only one sheet but there few more in pipeline which need to be worked upon). 5. retry with backoff then fail]

---

### B) Source Tabs & Expected Schema

For each tab (`IDFC-Bank`, `SBI-Bank`, `Payments`, `Packages`), provide:

6. Header row number (e.g., row 1).
7. Exact column names as they appear today.
8. Datatype expectation per column:

   - text / integer / decimal / date / timestamp / boolean.
9. Which columns are mandatory vs optional?
10. Any merged cells, formulas, filtered views, or hidden columns to handle? [None]
11. Are there footer rows / totals rows that must be ignored? [None]
12. Timezone of date/time fields in sheet data (IST? UTC? mixed?). [Only Date, No time zone]

[Answers 6,7,8,9]:

### 1. IDFC-Bank

**Header row:** 1

| Column Name      | Datatype | Mandatory | Notes                         |
| ---------------- | -------- | --------- | ----------------------------- |
| Bank             | text     | Yes       | Always "IDFC"                 |
| ROWID            | text     | Yes       | Unique transaction identifier |
| Transaction Date | date     | Yes       | Format: DD-MMM-YYYY           |
| Value Date       | date     | Optional  |                               |
| Particulars      | text     | Yes       | Bank narration                |
| Cheque No.       | text     | Optional  |                               |
| Debit            | decimal  | Optional  |                               |
| Credit           | decimal  | Optional  |                               |
| Balance          | decimal  | Optional  | Running balance               |
| Remarks          | text     | Optional  |                               |
| Cost Center      | text     | Optional  | Cost Center                   |
| Order Number     | text     | Optional  |                               |
| Category         | text     | Optional  | Income / Expense              |
| Sub Category     | text     | Optional  |                               |
| Branch Code      | text     | Optional  |                               |

**Validation Rules:**

- At least one of `Debit` or `Credit` must be present
- Both should not be populated simultaneously
- `(Bank + ROWID)` must be unique

---

#### 2. SBI-Bank

**Header row:** 1

| Column Name      | Datatype | Mandatory | Notes                             |
| ---------------- | -------- | --------- | --------------------------------- |
| Bank             | text     | Yes       | Always "SBI"                      |
| ROWID            | text     | Yes       | Unique transaction identifier     |
| Transaction Date | date     | Yes       | Format: DD-MMM-YY                 |
| Value Date       | date     | Optional  |                                   |
| Particulars      | text     | Yes       | Bank narration                    |
| Cheque No.       | text     | Optional  | Often contains transfer reference |
| Branch Code      | text     | Optional  |                                   |
| Debit            | decimal  | Optional  |                                   |
| Credit           | decimal  | Optional  |                                   |
| Balance          | decimal  | Optional  | Often blank                       |
| Remarks          | text     | Optional  |                                   |
| Cost Center      | text     | Optional  |                                   |
| Order Number     | text     | Optional  |                                   |
| Category         | text     | Optional  |                                   |
| Sub Category     | text     | Optional  |                                   |

**Validation Rules:**

- At least one of `Debit` or `Credit` must be present
- `(Bank + ROWID)` must be unique

---

#### 3. Payments

**Header row:** 1

| Column Name   | Datatype  | Mandatory | Notes                                                                                                             |
| ------------- | --------- | --------- | ----------------------------------------------------------------------------------------------------------------- |
| Timestamp     | timestamp | Yes       | Form submission timestamp                                                                                         |
| Email address | text      | Yes       |                                                                                                                   |
| Mode          | text      | Yes       | Cash / UPI / FranchiseUPI                                                                                         |
| Store         | text      | Yes       | Actual stored in this column is Cost Center                                                                       |
| Date          | date      | Yes       | Payment date                                                                                                      |
| Order Number  | text      | Yes       |                                                                                                                   |
| Amount        | decimal   | Yes       |                                                                                                                   |
| Remarks       | text      | Optional  |                                                                                                                   |
| ROWID         | text      | Optional  | The value in this column is not unique, this ROWID is supposed to be mapped with Bank ROWID during reconciliation |
| Handed OVer   | boolean   | Optional  | Yes / No/ blank                                                                                                   |
| Date Handed   | date      | Optional  |                                                                                                                   |
| date_modified | date      | Optional  | Last update date                                                                                                  |
| updated_flag  | boolean   | Optional  | TRUE / FALSE                                                                                                      |

**Validation Rules:**

- `Amount >= 0`
- `ROWID` may be "Cash" for cash entries
- Partial payments allowed (same Order Number multiple rows)

---

#### 4. Packages

**Header row:** 1

| Column Name   | Datatype | Mandatory | Notes                             |
| ------------- | -------- | --------- | --------------------------------- |
| Seq           | integer  | Yes       | Serial number                     |
| Cost Center   | text     | Yes       | Cost Center                       |
| Date          | date     | Yes       |                                   |
| Customer Name | text     | Yes       |                                   |
| Mobile Number | text     | Yes       | Keep as text (no numeric casting) |
| Address       | text     | Optional  | May be blank                      |
| Package Value | decimal  | Yes       |                                   |
| Payment Mode  | text     | Yes       | Cash / UPI / FranchiseUPI         |

**Validation Rules:**

- `Mobile Number` must remain text (no trimming leading zeros)
- `Package Value > 0`
- `Seq` should be unique within sheet

---

#### Global Assumptions (Important for Codex)

1. Header row is always row 1 across all tabs
2. Column names must be matched **exactly (case + space sensitive)**
3. Empty cells should be treated as `NULL`
4. Numeric values may come with commas and "." as decimal point → must be sanitized
5. Dates come in mixed formats → must be parsed carefully
6. No strict foreign key enforcement at ingestion stage
7. Deduplication key:
   - Bank tabs → `(Bank + ROWID)`
   - Payments → `(Mode+Store+Order Number+Amount)` (best effort)
   - Packages → `(Seq)`

---

#### RAW DATA SNAPSHOT (AS-IS FROM SHEETS)

##### 1. SBI-Bank

Bank	ROWID	Transaction Date	Value Date	Particulars	Cheque No.	Branch Code	Debit	Credit	Balance	Remarks	Cost Center	Order Number	Category	Sub Category
SBI	TSV01-0001	30-Jan-25	30-Jan-25	CASH DEPOSIT-CASH DEPOSIT SELF--	/	16446		500000		Seed Capital	TSV001		Income	Seed Capital
SBI	TSV01-0002	31-Jan-25	31-Jan-25	TO TRANSFER-UPI/DR/100084103340/GHAZALA /NA/9871023366/Pay to--	TRANSFER TO 4897695162091 /	16532	5000			Test Transfer	TSV001		Expense	Miscellaneous
SBI	TSV01-0003	31-Jan-25	31-Jan-25	BY TRANSFER-UPI/CR/503186009041/GHAZALA /HDFC/9871023366/Sent--	TRANSFER FROM 4897737162096 /	16532		5000		Reversal of Test Trasaction	TSV001		Income	Seed Capital

---

#### 2. IDFC-Bank

Bank	ROWID	Transaction Date	Value Date	Particulars	Cheque No.	Debit	Credit	Balance	Remarks	Cost Center	Order Number	Category	Sub Category	Branch Code
IDFC	TSV-06-0001	25-Jun-2025	26-Jun-2025	BB/CHQ DEP/574557/19-06-2025/IDFC/STATE BANK OF IN	574557		2,00,000.00	2,00,000.00	Initial Account Opening Money	TSV001		Income	Seed Capital	110751038
IDFC	TSV-06-0002	27-Jun-2025	27-Jun-2025	UPI/MOB/517878209356/Test UPI			100	2,00,100.00	Test UPI Txn	TSV001		Income	Seed Capital	110751038

---

##### 3. Payments

Timestamp	Email address	Mode	Store	Date	Order Number	Amount	Remarks	ROWID	Handed OVer	Date Handed	date_modified	updated_flag
08/12/2025 12:39:41	wagid.sheikh@gmail.com	UPI	KN3817	08/12/2025	T1096	240	Delivery by Rider Ankit	TSV-11-0526			30/04/2026	TRUE
08/12/2025 14:27:34	wagid.sheikh@gmail.com	UPI	SC3567	08/12/2025	UC567-1080	190	Delivery by Rider Shanne	TSV-11-0528			30/04/2026	TRUE

---

##### 4. Packages

Seq	Cost Center	Date	Customer Name	Mobile Number	Address	Package Value	Payment Mode
1	KN3817	01-Mar-2026	Gourav Bhardwaj	8800681863	Q Appartment 271 Dlf Capital Greens Moti Nagar Karampura Industrial Area Karam Pura	10000	UPI
2	KN3817	01-Mar-2026	Devika Kawatra	9999736673	K32 Kirti Nagar Kirti Nagar Delhi India	2000	UPI

---

### C) Destination PostgreSQL Mapping

13. Should we create **new tables** for this pipeline, or map into existing tables? [should be creating new tables]

- If existing, provide table names.

14. If creating new tables, confirm preferred names (proposal):
    * **IMPORTANT**: Data of IDFC-Bank tab and SBI-Bank will be inserted into a single table

- `bank_records`
```sql
CREATE TABLE IF NOT EXISTS bank_records (
    bank_record_id  BIGSERIAL PRIMARY KEY,

    bank               VARCHAR(30) NOT NULL,
    rowid              VARCHAR(30) NOT NULL,

    transaction_date   DATE NOT NULL,
    value_date         DATE,

    particulars        TEXT,
    cheque_no          TEXT,
    branch_code        VARCHAR(30),

    debit              NUMERIC(14,2),
    credit             NUMERIC(14,2),
    balance            NUMERIC(14,2),

    remarks            TEXT,
    cost_center        VARCHAR(8),
    order_number       VARCHAR(50),

    category           VARCHAR(50),
    sub_category       VARCHAR(100),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_bank_records_bank_rowid
        UNIQUE (bank, rowid)
);
CREATE INDEX IF NOT EXISTS idx_bank_records_transaction_date
ON bank_records (transaction_date);

CREATE INDEX IF NOT EXISTS idx_bank_records_value_date
ON bank_records (value_date);

CREATE INDEX IF NOT EXISTS idx_bank_records_bank
ON bank_records (bank);

CREATE INDEX IF NOT EXISTS idx_bank_records_cost_center
ON bank_records (cost_center);

CREATE INDEX IF NOT EXISTS idx_bank_records_order_number
ON bank_records (order_number);

CREATE INDEX IF NOT EXISTS idx_bank_records_category
ON bank_records (category);

CREATE INDEX IF NOT EXISTS idx_bank_records_sub_category
ON bank_records (sub_category);

CREATE INDEX IF NOT EXISTS idx_bank_records_bank_transaction_date
ON bank_records (bank, transaction_date);

CREATE INDEX IF NOT EXISTS idx_bank_records_bank_rowid
ON bank_records (bank, rowid);
```
- `payments_records`
```sql
CREATE TABLE IF NOT EXISTS payment_records (
    payment_record_id BIGSERIAL PRIMARY KEY,

    source_timestamp  TIMESTAMPTZ,
    email_address     VARCHAR(255),

    payment_mode      VARCHAR(16) NOT NULL,
    cost_center       VARCHAR(8) NOT NULL, -- column "Store" in Google Sheet is actually cost center
    payment_date      DATE NOT NULL,

    order_number      VARCHAR(50) NOT NULL,
    amount            NUMERIC(14,2) NOT NULL,

    remarks           TEXT,
    rowid             VARCHAR(50),

    handed_over       BOOLEAN,
    date_handed       DATE,

    date_modified     DATE,
    updated_flag      BOOLEAN,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_payment_records_payment_mode
        CHECK (payment_mode IN ('Cash', 'UPI', 'FranchiseUPI','Package','CC','DC','Other'))
);
CREATE INDEX IF NOT EXISTS idx_payment_records_payment_date
ON payment_records (payment_date);

CREATE INDEX IF NOT EXISTS idx_payment_records_store
ON payment_records (cost_center);

CREATE INDEX IF NOT EXISTS idx_payment_records_order_number
ON payment_records (order_number);

CREATE INDEX IF NOT EXISTS idx_payment_records_rowid
ON payment_records (rowid);
```
- `mst_package`
```sql
CREATE TABLE IF NOT EXISTS mst_package (
    package_record_id BIGSERIAL PRIMARY KEY,

    seq               INTEGER NOT NULL,
    package_code      VARCHAR(16) NOT NULL,
    cost_center       VARCHAR(8) NOT NULL,
    package_date      DATE NOT NULL,

    customer_name     VARCHAR(150) NOT NULL,
    mobile_number     VARCHAR(10) NOT NULL,
    address           TEXT,

    package_value     NUMERIC(14,2) NOT NULL,
    payment_mode      VARCHAR(16) NOT NULL,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_mst_package_seq
        UNIQUE (seq),

    CONSTRAINT chk_mst_package_package_value_positive
        CHECK (package_value > 0),

    CONSTRAINT chk_mst_package_payment_mode
        CHECK (payment_mode IN ('Cash', 'UPI', 'FranchiseUPI','Package','CC','DC','Other'))
);
CREATE INDEX IF NOT EXISTS idx_mst_package_package_date
ON mst_package (package_date);

CREATE INDEX IF NOT EXISTS idx_mst_package_cost_center
ON mst_package (cost_center);

CREATE INDEX IF NOT EXISTS idx_mst_package_mobile_number
ON mst_package (mobile_number);
```
- `bank_payment_reconciliation` (phase 2 output)
   - to be defined by Codex
- `tsv_bank_payment_sync_log` (optional detailed audit)
   - to be defined by Codex

15. For each source tab column, provide destination DB column name mapping.

15.1 IDFC-Bank → bank_records

| Source Tab | Source Column | Destination Table | Destination Column |
|---|---|---|---|
| IDFC-Bank | Bank | bank_records | bank |
| IDFC-Bank | ROWID | bank_records | rowid |
| IDFC-Bank | Transaction Date | bank_records | transaction_date |
| IDFC-Bank | Value Date | bank_records | value_date |
| IDFC-Bank | Particulars | bank_records | particulars |
| IDFC-Bank | Cheque No. | bank_records | cheque_no |
| IDFC-Bank | Debit | bank_records | debit |
| IDFC-Bank | Credit | bank_records | credit |
| IDFC-Bank | Balance | bank_records | balance |
| IDFC-Bank | Remarks | bank_records | remarks |
| IDFC-Bank | Cost Center | bank_records | cost_center |
| IDFC-Bank | Order Number | bank_records | order_number |
| IDFC-Bank | Category | bank_records | category |
| IDFC-Bank | Sub Category | bank_records | sub_category |
| IDFC-Bank | Branch Code | bank_records | branch_code |

---

15.2 SBI-Bank → bank_records

| Source Tab | Source Column | Destination Table | Destination Column |
|---|---|---|---|
| SBI-Bank | Bank | bank_records | bank |
| SBI-Bank | ROWID | bank_records | rowid |
| SBI-Bank | Transaction Date | bank_records | transaction_date |
| SBI-Bank | Value Date | bank_records | value_date |
| SBI-Bank | Particulars | bank_records | particulars |
| SBI-Bank | Cheque No. | bank_records | cheque_no |
| SBI-Bank | Branch Code | bank_records | branch_code |
| SBI-Bank | Debit | bank_records | debit |
| SBI-Bank | Credit | bank_records | credit |
| SBI-Bank | Balance | bank_records | balance |
| SBI-Bank | Remarks | bank_records | remarks |
| SBI-Bank | Cost Center | bank_records | cost_center |
| SBI-Bank | Order Number | bank_records | order_number |
| SBI-Bank | Category | bank_records | category |
| SBI-Bank | Sub Category | bank_records | sub_category |

---

15.3 Payments → payment_records

| Source Tab | Source Column | Destination Table | Destination Column |
|---|---|---|---|
| Payments | Timestamp | payment_records | source_timestamp |
| Payments | Email address | payment_records | email_address |
| Payments | Mode | payment_records | payment_mode |
| Payments | Store | payment_records | cost_center |
| Payments | Date | payment_records | payment_date |
| Payments | Order Number | payment_records | order_number |
| Payments | Amount | payment_records | amount |
| Payments | Remarks | payment_records | remarks |
| Payments | ROWID | payment_records | rowid |
| Payments | Handed OVer | payment_records | handed_over |
| Payments | Date Handed | payment_records | date_handed |
| Payments | date_modified | payment_records | date_modified |
| Payments | updated_flag | payment_records | updated_flag |

---

15.4 Packages → mst_package

| Source Tab | Source Column | Destination Table | Destination Column |
|---|---|---|---|
| Packages | Seq | mst_package | seq |
| Packages | Cost Center | mst_package | cost_center |
| Packages | Date | mst_package | package_date |
| Packages | Customer Name | mst_package | customer_name |
| Packages | Mobile Number | mst_package | mobile_number |
| Packages | Address | mst_package | address |
| Packages | Package Value | mst_package | package_value |
| Packages | Payment Mode | mst_package | payment_mode |

---

15.5 Derived / System Columns

| Destination Table | Destination Column | Source |
|---|---|---|
| mst_package | package_record_id | Auto-generated by PostgreSQL |
| mst_package | package_code | Must be generated by sync logic or manually added; no current source column exists |
| mst_package | created_at | Auto-generated by PostgreSQL |
| mst_package | updated_at | Auto-generated by PostgreSQL |
| payment_records | payment_record_id | Auto-generated by PostgreSQL |
| payment_records | created_at | Auto-generated by PostgreSQL |
| payment_records | updated_at | Auto-generated by PostgreSQL |
| bank_records | bank_record_id | Auto-generated by PostgreSQL |
| bank_records | created_at | Auto-generated by PostgreSQL |
| bank_records | updated_at | Auto-generated by PostgreSQL |

16. Upsert strategy: [update on key conflict]

- insert-only,
- update on key conflict,
- soft-delete missing rows,
- hard-delete missing rows (not recommended).

17. Should we store raw source row JSON for audit/debug? (recommended: yes): Yes
18. Volume estimate:

- rows per tab
   - IDFC-Bank: 4000+ [grows by 400-500 rows per month approx]
   - SBI-Bank: 1000 (approx, grows by 10-15 rows per month)
   - Payments: 2000+ [grows by approx 1000 rows per month]]
   - Packages: 30+ [Grows by 15-20 rows per month]
- daily growth,
- full refresh vs incremental approach [since we have de-dupe keys in place, we should perform full refresh]

---

### D) Pipeline Runtime Behavior

19. Execution frequency:

- hourly / every X minutes / daily (time + timezone): Every 2-3 hours

20. Backfill behavior:

- first run should load full history? [full]
- or last N days only?

21. Should pipeline be:

- fully transactional per tab, 
- or best-effort with partial success across tabs?
[best-effort with partial success across tabs]
22. Required observability:

- run summary counts by tab,
- inserted/updated/skipped/error counts,
- sample error rows in log table.
[run summary counts by tab, inserted/updated/skipped/error counts]
23. Notification behavior:

- on failure only,
- on success + failure,
- recipients/profile (from existing `pipelines`/`notification_profiles` setup).
[on success + failure, setup notification profile and email to be sent to: wagid.sheikh@gmail.com]
---

### E) De-duplication Rules (Phase 2 foundation)

24. Define dedupe keys per tab.
- Already Defined above

25. If duplicate records conflict, which source wins?

- latest row in sheet,
- first seen,
- bank record overrides sales record, etc.
[latest row in sheet]
26. Should dedupe happen:

- during ingest,
- during reconciliation,
- both?
[both]
---

### F) Reconciliation Logic (Phase 2 core)
I will explain full reconciliation logic once we begin with phase 2.
27. What is the canonical sales source table name in PostgreSQL?
   - There are two tables:
      - "orders": this table actually represents orders that were received from customers. A zero value is a valid order. We offer one free article zero cost to customer.
      - "sales": this table actually represents once "orders" are paid for by the customers. 
28. What identifies an order uniquely in sales table? (`order_id`? invoice no?).
   - cost_center + order_number
29. Online payment reconciliation matching priority (confirm order):
1) exact `utr/reference_id` [we do not have utr/reference_id in 99% of the cases]
2) exact `order_id`
3) amount + date window ±N days
4) customer/mobile fallback

30. Cash reconciliation logic:

- Which sheet tab records cash?
- What constitutes “actual cash received” proof?

31. Tolerance rules:

- exact amount only, or small variance allowed (e.g., ±1 INR rounding).

32. Date tolerance for matching:

- same day only, or ±1/2/3 days.

33. Status taxonomy expected in output:

- `MATCHED`
- `MISMATCH_AMOUNT`
- `MISSING_IN_BANK`
- `MISSING_IN_SALES`
- `DUPLICATE`
- `POSSIBLE_MATCH`
- (confirm/add)

34. “Marked as paid but actually unpaid” definition:

- sales status indicates paid,
- but no matched bank/cash evidence within tolerance window.

35. “Actually paid but not marked paid” definition:

- bank/cash evidence exists,
- but sales status unpaid/partial.

36. “Today’s actual collections vs marked in sales”:

- define “Today” timezone (likely `Asia/Kolkata`).
- output should include:
  - total actual collected
  - total marked collected
  - difference
  - order-level mismatch list.

---

### G) system_config Entries (to be created)

Please confirm you want these keys (names can be adjusted):

37. `TSV_BANK_PAYMENT_SYNC_ENABLED` (bool)
38. `TSV_BANK_PAYMENT_SYNC_SHEET_ID` (string)
39. `TSV_BANK_PAYMENT_SYNC_TABS` (JSON array)
40. `TSV_BANK_PAYMENT_SYNC_CREDENTIALS_JSON` (encrypted JSON)or `TSV_BANK_PAYMENT_SYNC_CREDENTIALS_REF` (secret reference)
41. `TSV_BANK_PAYMENT_SYNC_SCHEDULE_CRON` (string)
42. `TSV_BANK_PAYMENT_SYNC_TIMEZONE` (string, e.g., `Asia/Kolkata`)
43. `TSV_BANK_PAYMENT_SYNC_DEDUPE_RULES` (JSON)
44. `TSV_BANK_PAYMENT_SYNC_RECON_RULES` (JSON)
45. `TSV_BANK_PAYMENT_SYNC_DATE_FORMATS` (JSON)
46. `TSV_BANK_PAYMENT_SYNC_FAIL_ON_TAB_ERROR` (bool)
47. `TSV_BANK_PAYMENT_SYNC_MAX_RETRIES` (int)
48. `TSV_BANK_PAYMENT_SYNC_RETRY_BACKOFF_SEC` (int)
49. `TSV_BANK_PAYMENT_SYNC_NOTIFY_PROFILE` (string/pipeline code mapping)

---

### H) Data Quality / Edge Cases

50. Can negative amounts appear (refunds/reversals/chargebacks)?
51. Can one order have split payments (multiple transactions)?
52. Can one bank transaction settle multiple orders (bulk settlement)?
53. Are there known inconsistent order IDs (prefixes/spaces/case differences)?
54. Should we normalize:

- whitespace,
- casing,
- phone formatting,
- decimal rounding,
- date parsing from mixed formats?

55. Retention policy:

- keep all historical ingested rows forever,
- or archive/purge after N months.

---

## What I will produce after you answer

After you fill answers in this file, I will produce:

1. Final functional requirement spec for `tsv_bank_payment_sync`.
2. `system_config` contract (exact key/value schema).
3. Proposed DB schema + indexes + dedupe constraints.
4. Pipeline flow design (ingest -> normalize -> upsert -> reconcile -> report).
5. Reconciliation rulebook (online/cash/unpaid/overpaid/today collections).
6. Run summary + notification contract aligned to existing pipeline patterns.
7. Implementation task breakdown for Phase 1 and Phase 2.
