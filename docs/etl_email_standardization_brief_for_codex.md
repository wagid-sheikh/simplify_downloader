# ETL Notification Email Standardization Brief for Codex

## Objective

Standardize the ETL notification emails generated during the pipeline cycle so they are:

- easy to read in 5–10 seconds
- consistent across UC and TD pipelines
- suitable for plain-text email clients
- clean enough for operational monitoring
- enterprise-grade without becoming verbose
- controlled through DB template values updated via Alembic migration scripts

This document is intended to be handed directly to Codex for implementation.

---

## Current Problem

The current ETL emails contain useful data, but the formatting is inconsistent and noisy.

### Main issues observed

1. **UC and TD emails use different layouts**
   - Different section ordering
   - Different timestamp formats
   - Different naming conventions
   - Different placement of filenames and warnings

2. **Content is duplicated**
   - Some headers and run summaries appear more than once
   - TD example repeats the summary block

3. **Too much operational noise**
   - Debug-style metadata is mixed into the body
   - Reconciliation formulas are shown in the email
   - Row-level facts and repeated file references make scanning harder

4. **Subject lines are weak**
   - They do not clearly show environment, status, run date, and store code together

5. **Email is not optimized for fast operational review**
   - A monitoring email should allow the recipient to answer these questions immediately:
     - Which pipeline ran?
     - Which store ran?
     - Was it successful?
     - What date was processed?
     - Were there warnings or dropped rows?
     - Which files were processed?

---

## Design Goal

Create one consistent enterprise-grade pattern for ETL notification emails for all td_orders_sync & uc_orders_sync pipelines.

The standard should be:

- concise
- deterministic
- easy to scan
- plain-text friendly
- reusable across UC, TD, and future pipelines
- implementable by updating DB-stored subject/body templates through Alembic migrations

---

## Required Subject Line Standard

### New subject line format

```text
ETL - [{ENV}][{STATUS}][{STORE_CODE}] {PIPELINE_NAME} – {RUN_DATE}
```

### Examples

```text
[DEV][SUCCESS][UC567] UC Orders Sync – 2026-03-15
[PROD][SUCCESS][A817] TD Orders & Sales Sync – 2026-03-15
[PROD][FAILED][A668] TD Orders & Sales Sync – 2026-03-15
```

### Notes

- `ENV` should be uppercased, for example `DEV`, `PROD`
- `STATUS` should be uppercased, for example `SUCCESS`, `WARNING`, `FAILED`
- `STORE_CODE` must always be included when the run is store-specific
- `PIPELINE_NAME` should be human-readable and standardized
- `RUN_DATE` should stay in `DD-MMM-YYYY`

---

## Required Body Layout Standard

The email body must use the following sections in this exact order:

1. `PIPELINE RUN SUMMARY`
2. `WINDOW STATUS`
3. `STORE PROCESSING SUMMARY`
4. `FILES PROCESSED`
5. `WARNINGS`
6. `NOTES` (optional)

### Formatting rule

Use a plain-text layout that aligns well in monospaced view and remains readable in standard email clients.

---

## Required Timestamp Standard

All email timestamps must be shown in India time for operational readability.

### Standard format

```text
DD-MMM-YYYY HH:MM:SS IST
```

### Example

```text
16-MAR-2026 21:16:06 IST
```

### Important

- Do not mix UTC ISO strings in one email and localized display format in another
- Convert timestamps consistently before rendering the email body

---

## What Must Be Removed from the Email Body

These items should not appear in the main email body:

- duplicated summary blocks
- duplicated header blocks
- raw internal debug metadata
- reconciliation formulas
- row-level debug facts
- repeated filename sections
- verbose "Header:" dump blocks
- empty placeholder blocks that add no value

These items belong in logs, structured run records, or admin/debug views — not in the notification email.

---

## Standard Output Layout

### 1) PIPELINE RUN SUMMARY

```text
PIPELINE RUN SUMMARY
────────────────────────────────

Pipeline      : TD Orders & Sales Sync
Environment   : PROD
Store Code    : A817
Run ID        : 20260316_154605933465_A817_001
Run Date   : 2026-03-15

Start Time    : 2026-03-16 21:16:06 IST
Finish Time   : 2026-03-16 21:20:28 IST
Duration      : 00:04:22

Overall Status: SUCCESS
```

### 2) WINDOW STATUS

```text
WINDOW STATUS
────────────────────────────────

Windows Completed : 1 / 1
Missing Windows   : 0
```

### 3) STORE PROCESSING SUMMARY

#### UC example

```text
STORE PROCESSING SUMMARY
────────────────────────────────

Store: UC567
Status: SUCCESS

Rows Downloaded : 6
Rows Ingested   : 6
Inserted        : 1
Updated         : 5
Dropped         : 0
Warnings        : 0
```

#### TD example

```text
STORE PROCESSING SUMMARY
────────────────────────────────

Store: A817
Status: SUCCESS

ORDERS
Rows Downloaded : 22
Rows Ingested   : 22
Inserted        : 0
Updated         : 22
Dropped         : 0
Warnings        : 0

SALES
Rows Downloaded : 24
Rows Ingested   : 24
Inserted        : 0
Updated         : 24
Dropped         : 0
Warnings        : 0
Edited          : 0
Duplicates      : 0

GARMENTS/Order Line Items
Rows Downloaded : 24
Rows Ingested   : 24
Inserted        : 0
Updated         : 24
Dropped         : 0
Warnings        : 0
```

### 4) FILES PROCESSED

```text
FILES PROCESSED
────────────────────────────────

A817_td_orders_20260315_20260316.xlsx
A817_td_sales_20260315_20260316.xlsx
```

### 5) WARNINGS

If there are no warnings:

```text
WARNINGS
────────────────────────────────

None
```

If warnings exist:

```text
WARNINGS
────────────────────────────────

A817 — 2 rows dropped due to invalid customer_id
A817 — 1 sales row skipped because order reference was missing
```

### 6) NOTES

Only include this section when there is something useful to say.

Example:

```text
NOTES
────────────────────────────────

Run completed successfully with no issues recorded.
```

If there is no meaningful note, omit the `NOTES` section completely.

---

## Enterprise-Grade Rules

The final templates must follow these rules:

1. **One screen summary first**
   - Most important information must appear at the top

2. **No duplication**
   - Any summary block should appear only once

3. **Deterministic naming**
   - Same labels, same order, same formatting every time

4. **Plain-text first**
   - Do not rely on HTML for readability

5. **Readable by operators, not just developers**
   - The email is for monitoring and action, not debugging

6. **Warnings must stand out**
   - If there are warnings, they should be listed in one clear section

7. **Body length should be controlled**
   - Keep the email concise
   - Avoid dumping raw internals into the email

---

## Suggested Final Templates

### Suggested subject template

```text
[{env_upper}][{overall_status_upper}][{store_code}] {pipeline_display_name} – {RUN_DATE}
```

## Explicit Implementation Instruction for Codex

### Important architecture note

The email subject and body template values come from a database table.

Therefore:

- do **not** hardcode the final templates in application logic if the system is already template-driven through DB configuration
- update the relevant DB template column values using **Alembic migration scripts**
- keep application rendering logic compatible with the new placeholders and formatting
- only adjust application code where necessary to support:
  - standardized placeholders
  - store code in subject line
  - IST timestamp formatting
  - optional sections like `NOTES`
  - clean rendering of warning/file/store summary blocks

---

## Mandatory Alembic Migration Requirement

Codex must implement the template change through Alembic migrations.

### Required migration behavior

1. Create a new Alembic revision
2. Update the relevant DB table rows that store:
   - email subject template
   - email body template
3. Apply changes for both:
   - UC store notification template(s)
   - TD store notification template(s)
4. Preserve existing template identity/keys unless there is a very strong reason to rename them
5. Add a safe downgrade path restoring the previous template values if feasible
6. Do not manually patch production DB outside migration scripts

---

## Explicit Comments for Codex to Follow

### Comment 1 — inspect current template source
Before changing anything, locate the exact DB table and columns that currently store:
- subject template
- body template
- template key / event key / pipeline key
- environment-specific variants, if any

### Comment 2 — preserve template engine compatibility
Do not invent placeholder syntax unless it matches the existing renderer.
Reuse the project’s current templating style.

### Comment 3 — keep logic/template responsibility clean
- Data preparation should happen in code
- Presentation structure should live in the DB template
- Complex derived blocks like warnings, file list, and per-store metric block may be pre-rendered in code and injected into the template

### Comment 4 — normalize display fields before render
Prepare these display-ready fields before rendering:
- `env_upper`
- `overall_status_upper`
- `started_at_ist`
- `finished_at_ist`
- `pipeline_display_name`
- `store_code`
- `files_processed_block`
- `warnings_block`
- `store_processing_summary_block`
- `optional_notes_block`

### Comment 5 — do not expose debug-only internals
Anything meant for developer diagnostics should remain in structured logs, not email template output.

### Comment 6 — make templates future-safe
The structure should be reusable for future ETL notifications beyond UC and TD.

---

## Recommended Implementation Approach

1. Identify current template records in DB
2. Review how placeholders are currently rendered
3. Add or normalize any missing render-context fields in code
4. Create Alembic migration to update subject/body template values
5. Validate output for:
   - UC success run
   - TD success run
   - warning case
   - failed case
6. Ensure no duplicate blocks remain
7. Ensure subject line includes store code

---

## Acceptance Criteria

The change is complete only when all of the following are true:

- subject includes environment, status, store code, pipeline name, and run date
- UC and TD emails follow the same section ordering
- timestamps are consistently rendered in IST
- duplicate content is removed
- warnings are shown in one clear section
- filenames appear only in `FILES PROCESSED`
- debug/reconciliation noise is removed from email body
- templates are stored via DB values updated by Alembic migration
- output is readable in plain text without relying on HTML styling

---

## Copy-Paste Task for Codex

```text
Standardize the ETL notification email templates stored in the database.

Goal:
Make UC and TD ETL emails consistent, concise, readable, and enterprise-grade.

Required subject format:
[{ENV}][{STATUS}][{STORE_CODE}] {PIPELINE_NAME} – {RUN_DATE}

Required body section order:
1. PIPELINE RUN SUMMARY
2. WINDOW STATUS
3. STORE PROCESSING SUMMARY
4. FILES PROCESSED
5. WARNINGS
6. NOTES (optional)

Implementation requirements:
- update DB template column values through Alembic migration scripts
- do not hardcode final template text in application logic if DB-driven templates already exist
- preserve current template engine placeholder style
- add/normalize render-context values in code only where required
- standardize timestamps to YYYY-MM-DD HH:MM:SS IST
- remove duplicated blocks, debug dumps, reconciliation formulas, and row-level debug noise from emails
- ensure filenames are shown only under FILES PROCESSED
- ensure warnings are shown only under WARNINGS
- include store code in subject line

Also add a safe downgrade in the Alembic migration if practical.
```

---

## Final Note

This is not just a cosmetic change. This is an operational readability improvement for ETL monitoring.

The final result should allow the recipient to determine run health, store identity, processed date, and any actionable issues almost instantly.
