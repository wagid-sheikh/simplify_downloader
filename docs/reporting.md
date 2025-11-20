## 🧾 Codex Task – Implement Daily PDF Store Reports + Email (V1)

You are working in the `app`-based CRM backend project (legacy name: `simplify_downloader`).

The **scraping + ingest pipeline is already implemented elsewhere** (single-session login, dashboard scraper, CSV download/merge/ingest into Postgres).

This task is **only** about:

1. Generating **per-store daily PDF reports** from existing DB data using a given HTML template.
2. Optionally **emailing those PDFs**, controlled entirely by environment variables.
3. Keeping this logic **separate** from the existing download/scraper entrypoints.

Do **not** change the scraping / login / ingest flow.

---

## 1. Store Selection Rules (VERY IMPORTANT)

Use the database flags on `store_master`:

* ✅ Generate PDFs **only** for stores where `report_flag` is `TRUE`.
* ❌ If no stores are flagged, **do NOT generate any PDFs or send email**—log the skip.

`report_flag` is seeded from historical `REPORT_STORES_LIST` values during migration and is the
sole source of truth going forward.

---

## 2. Files to Create

You must create **two new modules**:

1. `dashboard_downloader/report_generator.py`  → **Library / service layer**
2. `dashboard_downloader/run_store_reports.py` → **Orchestrator / CLI entry**

They must work together but keep responsibilities separate.

---

## 3. `report_generator.py` – Library / Service Layer

This module should be a **reusable service**, with **no direct env or CLI parsing**.

Implement at least these functions:

```python
from datetime import date
from typing import Dict

def build_store_context(store_code: str, report_date: date, run_id: str) -> Dict:
    """
    Build the Jinja context for one store's daily report.

    Responsibilities:
    - Use existing DB utilities (same connection approach as other modules)
      to query the already-ingested data for this store + date.
      * Use the dashboard summary table(s) and CSV-backed tables that the
        scraping pipeline already fills (e.g. missed leads, undelivered,
        repeat customers). Reuse existing models/helpers where possible.

    - Compute the **8 KPIs**:

        PRIMARY:
        1) pickup_conversion_total_pct
        2) pickup_conversion_new_pct
        3) delivery_tat_pct
        4) undelivered_10_plus_count
        5) undelivered_total_count
        6) repeat_customer_pct

        SECONDARY:
        7) ftd_revenue
        8) high_value_orders_count

    - Apply simple threshold rules (hard-coded dict or a small JSON config)
      to derive:
        - *_status_label (e.g. "Excellent", "Good", "Poor", "Critical")
        - *_status_class (matching the CSS classes used in store_report.html:
          e.g. kpi-status-excellent / kpi-status-good / kpi-status-poor / kpi-status-critical)
        - overall_health_score (0–100)
        - overall_health_label (string, e.g. "Stable", "At Risk")
        - overall_health_status_class (status-excellent / status-good / status-warning / status-critical)

    - Compute snapshot metrics for the "Today’s Operating Snapshot" table:
        - leads_total, leads_note
        - pickups_total, pickups_note
        - deliveries_total, deliveries_note
        - new_customers_count, new_customers_note
        - repeat_customers_count, repeat_customers_note

    - Build rule-based recommendation lists (simple if/else logic, no ML):
        - highlights: List[str]          # good things that happened today
        - focus_areas: List[str]         # risk/problem flags
        - actions_today: List[str]    # concrete same-day actions

      Use the KPI values + thresholds to decide which sentences to include.

    - Build a context dict matching the Jinja placeholders in
      templates/store_report.html, including:

        Header/meta:
          store_name
          store_code
          city
          report_date          # string, e.g. "2025-11-14"
          generated_at         # string datetime
          run_id               # pipeline run id
          logo_src             # data URL or None

        Overall health:
          overall_health_score
          overall_health_label
          overall_health_status_class
          overall_health_summary   # short 1–2 sentence summary

        KPI fields:
          pickup_conversion_total_pct
          pickup_conversion_total_status_label
          pickup_conversion_total_status_class
          pickup_conversion_new_pct
          pickup_conversion_new_status_label
          pickup_conversion_new_status_class
          delivery_tat_pct
          delivery_tat_status_label
          delivery_tat_status_class
          undelivered_10_plus_count
          undelivered_10_plus_status_label
          undelivered_10_plus_status_class
          undelivered_total_count
          undelivered_total_status_label
          undelivered_total_status_class
          repeat_customer_pct
          repeat_customer_status_label
          repeat_customer_status_class
          ftd_revenue
          high_value_orders_count

        Snapshot metrics:
          leads_total, leads_note
          pickups_total, pickups_note
          deliveries_total, deliveries_note
          new_customers_count, new_customers_note
          repeat_customers_count, repeat_customers_note

        Recommendations:
          highlights
          focus_areas
          actions_today

    - For logo_src:
        - Either set None (so the template renders a placeholder),
          OR use a data URL: "data:image/png;base64,..." if you can
          easily embed a local logo. Keep this simple.

    Return the context dict. If there is no data for this store/date,
    raise a clear exception that the caller can handle.
    """
    ...


async def render_store_report_pdf(store_context: Dict, output_path: str, template_path: str | None = None) -> None:
    """
    Render a single store report to PDF.

    Responsibilities:
    - Use the `StoreReportPdfBuilder` (ReportLab + AcroForm) to render the
      entire report, including the interactive Undelivered Orders and Missed
      Leads sections.

    - Ensure the parent directory of output_path exists, e.g.:
        reports/YYYY-MM-DD/STORE_CODE.pdf

    - Do not handle email or env vars here.
    """
    ...
```

**Output path convention (enforced by the caller):**

* For `report_date = 2025-11-14` and `store_code = "A668"`:

```text
reports/2025-11-14/A668.pdf
```

---

## 4. `run_store_reports.py` – Orchestrator / CLI Entry

This module is a **thin orchestration layer** that:

* Parses env vars / CLI.
* Decides **which stores** and **which date**.
* Calls `report_generator.build_store_context` and `render_store_report_pdf`.
* Optionally sends an email with all PDFs attached.

### 4.1 CLI & main flow

Implement an `async` main entry that can be run as:

```bash
python -m dashboard_downloader.run_store_reports --report-date 2025-11-14
```

Behavior:

1. **Resolve report_date:**

   * If `--report-date YYYY-MM-DD` is provided:

     * Use that date.
   * Else:

     * Default to a reasonable value (e.g. “yesterday” or latest date in dashboard table) – pick one consistent strategy and stick with it.
2. **Resolve store_codes from the database:**

   * Query `store_master` for rows with `report_flag = TRUE`.
   * If the resulting list is empty:

     * Log (JSON):

       ```json
       {"phase":"report","status":"info","message":"no report-eligible stores found in store_master, skipping report generation"}
       ```
     * Do **not** generate PDFs.
     * Do **not** send email.
     * Exit successfully.
   * If non-empty, use that as the list of `store_codes` to generate reports for.
3. **For each store_code:**

   * Log report start, e.g.:

     ```json
     {
       "phase": "report",
       "status": "info",
       "message": "report generation start",
       "store_code": "A668",
       "extras": {"report_date": "2025-11-14", "run_id": "..."}
     }
     ```
   * Call `build_store_context(store_code, report_date, run_id)`.
   * Compute `output_path = f"reports/{report_date_iso}/{store_code}.pdf"`.
   * Await `render_store_report_pdf(store_context, template_path, output_path)`.
   * Log report success:

     ```json
     {
       "phase": "report",
       "status": "ok",
       "message": "report pdf generated",
       "store_code": "A668",
       "extras": {"report_date": "2025-11-14", "path": "reports/2025-11-14/A668.pdf"}
     }
     ```
   * If `build_store_context` raises a “no data for this store/date” style error, log an informative warning and skip that store (don’t crash the whole run).
4. **Collect a list of all successfully generated PDF paths** and their store_codes.
   This list will be used for email.

---

## 5. Email Sending (DB-Driven)

Email distribution is no longer controlled by `.env` recipient lists. Instead the
notification pipeline reads all metadata from Postgres after PDFs are recorded:

* `notification_profiles` identifies which pipelines and scopes should receive
  emails (e.g., `dashboard_daily` + `store_daily_reports`). Use the
  `dashboard_daily`, `dashboard_weekly`, and `dashboard_monthly` pipeline codes
  (formerly `simplify_dashboard_*`) in both notification profiles and
  `pipeline_run_summaries` so run metadata lines up with the dispatcher.
* `email_templates` stores the subject/body Jinja templates per profile.
* `notification_recipients` lists the To/Cc/Bcc rows per store and per
  environment.
* `documents` collects every PDF path created by `run_store_reports.py`.

`dashboard_downloader.notifications.send_notifications_for_run` stitches these
tables together once the run summary is written. The dispatcher will:

1. Query the latest run in `pipeline_run_summaries`.
2. Load all generated documents for that `run_id`.
3. Select active templates + recipients from the DB.
4. Send one email per plan (run summary + per-store PDFs) using SMTP settings
   from `REPORT_EMAIL_SMTP_*` / `REPORT_EMAIL_FROM`.

Operational rules:

* If **no PDFs** were written, the notifications layer still executes but only
  sends run-summary messages (no attachments) and logs
  `"summary-only notification scheduled"`.
* If PDFs exist but no active recipients/templates match, the dispatcher logs a
  diagnostic and exits without failing the pipeline.
* SMTP env vars now strictly define the transport (host, port, username,
  password, TLS). No other `REPORT_EMAIL_*` variables exist.
* Run-level logging remains the same (`phase="report_email"` entries describe
  whether notifications were queued, skipped, or failed), but sending logic is
  entirely database-driven.

This model guarantees that store-specific audience changes happen centrally in
the DB without redeploying the downloader.

---

## 6. Logging & Integration Expectations

* Use the same JSON logging style as existing pipelines:

  * keys like: `run_id`, `phase`, `status`, `message`, `store_code`, `extras`, `ts`.
* `phase` values should include at least:

  * `"report"` for PDF generation
  * `"report_email"` for email sending
* The new report runner must be callable as a standalone step, **separate from the scraper**, for example:

  * Scrape: `./scripts/run_dashboard_pipeline_single_context.sh`
  * Reports: `python -m dashboard_downloader.run_store_reports --report-date 2025-11-14`

Do **not** change existing pipeline entrypoints in this task.

---

**Implement exactly this behavior, with no fallback to `STORES_LIST` for reports, and with notifications sourced from the database (not `.env` recipient lists).**


---

## 7. First-Run / T-1 Data Missing Behavior (IMPORTANT)

On the **first day** we run reports for a store, there may be **no T-1 (previous day) data** or historical baseline in the DB.

You **must not crash** or fail the report in this case.

**Rules:**

1. `build_store_context` MUST:

   * Still generate a report for **T (the requested `report_date`)** as long as there is **some data for that store on T** (e.g. today’s dashboard summary / pickups / deliveries).
   * Treat any **missing T-1 or historical fields** (used for comparisons, trends, or thresholds) as:

     * `None`, `0`, or `"N/A"` (pick one consistent representation per field), and
     * Adjust any derived text notes accordingly (e.g. “No previous data available for comparison yet.”).

2. Only raise the “no data for this store/date” style exception if:

   * There is **no usable data at all** for the requested `report_date` T for that store (i.e. not even today’s dashboard summary row).

3. `run_store_reports.py` MUST:

   * Handle this “no data for this store/date” exception gracefully:

     * Log a clear warning for that store.
     * Skip that store.
     * Continue with other stores without crashing the whole run.

In short:

> If there is **T data but no T-1** → ✅ generate report for T with limited comparisons.
> If there is **no T data** for that store/date → ⚠️ log and skip, do not crash the job.

---

Here is the **cleanest possible instruction** to add to the Codex task so that the daily PDF reporting module **automatically wires itself into the existing single-session pipeline**, without breaking isolation or duplicating login logic.

Use this as an **add-on section** (again, no code):

---

## 8. Automatic Triggering in Single-Session Pipeline (Wiring Rule)

When this report module (V1) is implemented, it **must integrate cleanly** with the existing **single-session scraping/ingest pipeline** *without duplicating login logic* and **without creating a second browser session**.

**Rules:**

1. The report runner (`run_store_reports.py`) is a **separate entrypoint**, but:

   * It must be designed so that it can be **optionally invoked as the final step** of the existing pipeline.
   * The single-session pipeline should be able to call:

     ```bash
     python -m dashboard_downloader.run_store_reports --report-date <DATE>
     ```

     **in the same process run** *after* scraping is finished.

2. The report runner **must NOT**:

   * Perform any scraping,
   * Perform any login,
   * Launch Playwright except for PDF generation (HTML → PDF only),
   * Touch the browser context used by scraping.

3. The existing pipeline will **reuse the same run_id** and pass it to the report runner (if invoked inside the same session), so:

   * All JSON logs from the report generation step must include the inherited run_id.
   * This keeps the entire scraping → ingest → report chain traceable as a **single logical run**.

4. No special coupling is required:

   * The single-session script will simply call the report runner as **the last step** using `subprocess` or direct Python invocation.
   * Therefore, the report runner must be **pure**, **standalone**, and **idempotent**, relying only on:

     * the DB,
     * HTML template,
     * env vars.

5. The report runner must be designed to:

   * Execute correctly whether invoked:

     1. **Manually**
     2. **From cron**
     3. **Automatically by the scraping pipeline**

   Without assuming anything about browser state, login state, or cookies.

**In summary:**

> Once implemented, the report module should run automatically at the end of the single-session scraping pipeline by simply invoking the new report entrypoint. No login and no scraping must occur again. The PDF and email generation must operate purely from the ingested DB data in the same run.

---

## 10. Future TODO's
After completing the task, update .env.example to ensure all needed variables are present that must be configured in .env
---
## 11. Future TODO's
DO NOT DEVELOP THIS YET, this section is just a roadmap for this sub-module
V2: Store-wise email routing
V3: Weekly + monthly summaries
V4: WhatsApp auto-send
V5: HTML report viewer in the TSV-RSM admin panel
V6: Multi-store combined analytics
Naming this V1 helps Codex and your future self know:
This is the foundational implementation.
Later tasks will extend it, not rewrite it.