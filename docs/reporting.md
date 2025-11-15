## 🧾 Codex Task – Implement Daily PDF Store Reports + Email (V1)

You are working in the `simplify_downloader` project.

The **scraping + ingest pipeline is already implemented elsewhere** (single-session login, dashboard scraper, CSV download/merge/ingest into Postgres).

This task is **only** about:

1. Generating **per-store daily PDF reports** from existing DB data using a given HTML template.
2. Optionally **emailing those PDFs**, controlled entirely by environment variables.
3. Keeping this logic **separate** from the existing download/scraper entrypoints.

Do **not** change the scraping / login / ingest flow.

---

## 1. Store Selection Rules (VERY IMPORTANT)

Use the env var:

```env
REPORT_STORES_LIST="A668,A526"   # comma-separated store codes
```

**Rules (no fallbacks):**

* If `REPORT_STORES_LIST` is **set and non-empty**:

  * ✅ Generate PDFs **only** for those store codes.
* Else:

  * ❌ **Do NOT generate any PDFs.**
  * ❌ **Do NOT send any email.**
  * Just log that report generation is skipped because no report stores are configured.

`STORES_LIST` is used elsewhere for scraping and **must NOT** be used as a fallback for reports.

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
        - actions_tomorrow: List[str]    # concrete next-day actions

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
          actions_tomorrow

    - For logo_src:
        - Either set None (so the template renders a placeholder),
          OR use a data URL: "data:image/png;base64,..." if you can
          easily embed a local logo. Keep this simple.

    Return the context dict. If there is no data for this store/date,
    raise a clear exception that the caller can handle.
    """
    ...


async def render_store_report_pdf(store_context: Dict, template_path: str, output_path: str) -> None:
    """
    Render a single store report to PDF.

    Responsibilities:
    - Load the Jinja2 environment, point it to the templates directory, and
      load "store_report.html" from template_path (or from the template
      environment that includes that path).

    - Render HTML using:
        html = template.render(**store_context)

    - Use Playwright to:
        - Launch a headless browser
        - Create a new page
        - Set page content to the rendered HTML
        - Call page.pdf(...) with:
            format="A4"
            print_background=True
        - Save the PDF to output_path.

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
2. **Resolve store_codes from env:**

   ```env
   REPORT_STORES_LIST="A668,A526"
   ```

   * Read `REPORT_STORES_LIST` from env.
   * Parse it into a list of non-empty trimmed codes.
   * If the resulting list is empty:

     * Log (JSON):

       ```json
       {"phase":"report","status":"info","message":"no REPORT_STORES_LIST configured, skipping report generation"}
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

## 5. Email Sending (Optional, Env-Controlled)

Email must be sent **only if**:

* At least **one PDF** was successfully generated, **and**
* Email env variables are sufficiently configured.

### 5.1 Env vars for email

Use the following env vars:

```env
REPORT_EMAIL_TO="boss@tsv.com,ops@tsv.com"   # comma-separated, REQUIRED to send
REPORT_EMAIL_CC=""                           # optional, comma-separated
REPORT_EMAIL_FROM="reports@tsv.com"          # sender address

REPORT_EMAIL_SUBJECT_TEMPLATE="[Store Reports] {{report_date}}"

REPORT_EMAIL_SMTP_HOST="smtp.example.com"
REPORT_EMAIL_SMTP_PORT="587"
REPORT_EMAIL_SMTP_USERNAME="reports@tsv.com"
REPORT_EMAIL_SMTP_PASSWORD="***"
REPORT_EMAIL_USE_TLS="true"                  # "true" or "false"
```

### 5.2 Behavior

In `run_store_reports.py`, after all PDFs are generated:

1. **If no PDFs** were generated for this run (empty list of files):

   * Do **not** send any email.
   * Log:

     ```json
     {
       "phase": "report_email",
       "status": "info",
       "message": "no reports generated, skipping email"
     }
     ```
   * Exit successfully.
2. **If there are PDFs** but email is not configured:

   * Conditions for “not configured”:

     * `REPORT_EMAIL_TO` is empty/missing, OR
     * `REPORT_EMAIL_SMTP_HOST` or `REPORT_EMAIL_SMTP_PORT` is missing.
   * Do **not** send email.
   * Log:

     ```json
     {
       "phase": "report_email",
       "status": "info",
       "message": "report_email: disabled or not configured, skipping send"
     }
     ```
3. **If there are PDFs and email is configured:**

   * Build one email:

     * From: `REPORT_EMAIL_FROM`
     * To: list from `REPORT_EMAIL_TO`
     * Cc: list from `REPORT_EMAIL_CC` (if non-empty)
     * Subject: render `REPORT_EMAIL_SUBJECT_TEMPLATE` with `{{report_date}}`

       * Example: `[Store Reports] 2025-11-14`
     * Body: simple plain text, e.g.:

       ```
       Daily store health reports for {{report_date}}.

       Stores:
       - A668
       - A526

       Generated by TSV Simplify Downloader.
       ```
     * Attach **all generated PDFs** for this `report_date`:

       * One attachment per store, e.g. `A668.pdf`, `A526.pdf`.
   * Use standard SMTP with the given host, port, username, password:

     * If `REPORT_EMAIL_USE_TLS == "true"` → use TLS/STARTTLS.
     * Else → plain connection.
   * On success, log:

     ```json
     {
       "phase": "report_email",
       "status": "ok",
       "message": "report email sent",
       "extras": {
         "report_date": "2025-11-14",
         "store_codes": ["A668","A526"],
         "to": ["boss@tsv.com","ops@tsv.com"]
       }
     }
     ```
   * On failure, log:

     ```json
     {
       "phase": "report_email",
       "status": "error",
       "message": "report email failed",
       "extras": {"error": "<short reason>"}
     }
     ```

     but **do not delete or re-generate PDFs** and **do not crash** the whole script after PDFs are written.

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

**Implement exactly this behavior, with no fallback to `STORES_LIST` for reports, and with email strictly conditional on (PDFs exist) + (email env correctly set).**
