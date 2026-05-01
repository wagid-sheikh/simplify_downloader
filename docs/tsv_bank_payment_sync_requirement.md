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

---

### B) Source Tabs & Expected Schema

For each tab (`IDFC-Bank`, `SBI-Bank`, `Payments`, `Packages`), provide:

6. Header row number (e.g., row 1).
7. Exact column names as they appear today.
8. Datatype expectation per column:

   - text / integer / decimal / date / timestamp / boolean.
9. Which columns are mandatory vs optional?
10. Any merged cells, formulas, filtered views, or hidden columns to handle?
11. Are there footer rows / totals rows that must be ignored?
12. Timezone of date/time fields in sheet data (IST? UTC? mixed?).

---

### C) Destination PostgreSQL Mapping

13. Should we create **new tables** for this pipeline, or map into existing tables?

- If existing, provide table names.

14. If creating new tables, confirm preferred names (proposal):

- `tsv_bank_idfc_records`
- `tsv_bank_sbi_records`
- `tsv_payments_records`
- `tsv_packages_records`
- `tsv_bank_payment_reconciliation` (phase 2 output)
- `tsv_bank_payment_sync_log` (optional detailed audit)

15. For each source tab column, provide destination DB column name mapping.
16. Upsert strategy:

- insert-only,
- update on key conflict,
- soft-delete missing rows,
- hard-delete missing rows (not recommended).

17. Should we store raw source row JSON for audit/debug? (recommended: yes)
18. Volume estimate:

- rows per tab,
- daily growth,
- full refresh vs incremental approach.

---

### D) Pipeline Runtime Behavior

19. Execution frequency:

- hourly / every X minutes / daily (time + timezone).

20. Backfill behavior:

- first run should load full history?
- or last N days only?

21. Should pipeline be:

- fully transactional per tab,
- or best-effort with partial success across tabs?

22. Required observability:

- run summary counts by tab,
- inserted/updated/skipped/error counts,
- sample error rows in log table.

23. Notification behavior:

- on failure only,
- on success + failure,
- recipients/profile (from existing `pipelines`/`notification_profiles` setup).

---

### E) De-duplication Rules (Phase 2 foundation)

24. Define dedupe keys per tab.
    Example candidates (to confirm):

- Bank tabs: `txn_date + amount + reference_number + account_last4`
- Payments tab: `order_id + payment_mode + paid_amount + paid_date`
- Packages tab: `package_id` or `order_id + package_code`

25. If duplicate records conflict, which source wins?

- latest row in sheet,
- first seen,
- bank record overrides sales record, etc.

26. Should dedupe happen:

- during ingest,
- during reconciliation,
- both?

---

### F) Reconciliation Logic (Phase 2 core)

27. What is the canonical sales source table name in PostgreSQL?
28. What identifies an order uniquely in sales table? (`order_id`? invoice no?).
29. Online payment reconciliation matching priority (confirm order):

1) exact `utr/reference_id`
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
