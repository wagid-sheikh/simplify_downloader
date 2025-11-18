# TSV Daily Store Performance Report — KPI + Action Tables Specification (v1 — Interactive Action Lists (Final Specification Upon Approval)

### Codex-Ready Implementation Document (Strict Instructions — No Guesswork)

---

## 1. Purpose of This Document

This document defines the **complete and authoritative specification** for the
TSV Daily Store Performance Report.

It now covers **two tightly related deliverables**:

1. The existing **Daily Store Performance PDF** (summary + KPIs + recommendations).
2. A new **Store Action List PDF with interactive form fields** (two action tables)
   implemented using **ReportLab + AcroForm**.

Codex must use this document to:

* Align KPI calculations and bands.
* Align snapshot comparisons and colour coding.
* Add and wire **two action tables** into:

  * The **existing summary report HTML/PDF** (visual tables only), and
  * A **new interactive Action List PDF** built with **ReportLab + AcroForm**.
* Extend the existing context-building function and Jinja template only where
  specified.

### 1.1 Non-Negotiable Constraints

Codex **MUST NOT**:

* Create new KPIs beyond those defined here.
* Create new database tables or alter existing schema (no DDL).
* Change the semantics of `build_store_context(...)` beyond fields explicitly
  requested here.

Codex **MUST**:

* Enhance the current store performance report template and pipeline only.
* Use existing styles and structure, with minimal additions.
* Keep all changes tightly scoped to the features described below.
* Implement a **second PDF** (Action List) using **ReportLab + AcroForm** to
  provide truly editable checkboxes and comment boxes for the two action tables.

> **Important versioning note**: Earlier (previous draft versions — ignore) constraints that *forbade*
> interactive PDF form fields are now **superseded** in v2.4. In this version,
> interactive fields are **required**, but **only** in the Action List PDF,
> not in the main summary PDF.

---

## 2. KPI Direction ("Higher" vs "Lower")

A KPI’s **direction** defines whether higher numeric values are good or bad.

* `direction = "higher"` → Higher = better (e.g. conversion %, repeat %, TAT %).
* `direction = "lower"` → Lower = better (e.g. undelivered counts).

The existing implementation already uses this concept; Codex must **preserve**
that behaviour.

---

## 3. KPI Band Definitions (Final)

The following definitions **replace** any older thresholds in the code.

### 3.1 Pickup Conversion Total (%) — `pickup_conversion_total_pct`

| Range    | Label           | Score |
| -------- | --------------- | ----- |
| ≥ 82%    | Excellent       | 100   |
| 70–81.9% | Good            | 80    |
| 55–69.9% | Needs Attention | 55    |
| < 55%    | Critical        | 35    |

Direction: **higher**.

### 3.2 New Customer Conversion (%) — `pickup_conversion_new_pct`

| Range    | Label           | Score |
| -------- | --------------- | ----- |
| ≥ 80%    | Excellent       | 100   |
| 60–79.9% | Good            | 80    |
| 45–59.9% | Needs Attention | 55    |
| < 45%    | Critical        | 35    |

Direction: **higher**.

### 3.3 Existing Customer Conversion (%) — `pickup_conversion_existing_pct`

This is a new band to be created; the underlying data already exists as
`store_dashboard_summary.pickup_existing_conv_pct`.

| Range    | Label           | Score |
| -------- | --------------- | ----- |
| ≥ 95%    | Excellent       | 100   |
| 80–94.9% | Good            | 80    |
| 70–79.9% | Needs Attention | 55    |
| < 70%    | Critical        | 35    |

Direction: **higher**.

### 3.4 Delivery TAT (%) — `delivery_tat_pct`

| Range    | Label           | Score |
| -------- | --------------- | ----- |
| ≥ 95%    | Excellent       | 100   |
| 85–94.9% | Good            | 80    |
| 70–84.9% | Needs Attention | 55    |
| < 70%    | Critical        | 35    |

Direction: **higher**.

### 3.5 Undelivered >10 Days — `undelivered_10_plus_count`

Operationally, any order pending **more than 10 days** is very serious.

| Count | Label           | Score |
| ----- | --------------- | ----- |
| 0     | Excellent       | 100   |
| 1–2   | Needs Attention | 55    |
| ≥ 3   | Critical        | 35    |

There is **no “Good” band** for this KPI.

Direction: **lower**.

### 3.6 Total Undelivered Orders — `undelivered_total_count`

| Count | Label           | Score |
| ----- | --------------- | ----- |
| 0–2   | Excellent       | 100   |
| 3–5   | Good            | 80    |
| > 5   | Needs Attention | 55    |

Direction: **lower**.

### 3.7 Repeat Customer Contribution (%) — `repeat_customer_pct`

| Range    | Label           | Score |
| -------- | --------------- | ----- |
| ≥ 65%    | Excellent       | 100   |
| 50–64.9% | Good            | 80    |
| 35–49.9% | Needs Attention | 55    |
| < 35%    | Critical        | 35    |

Direction: **higher**.

---

## 4. Overall Health Score

The **overall health score** is the rounded average of all **non-missing** KPI
scores defined above.

| Average Score | Label     | Meaning                     |
| ------------- | --------- | --------------------------- |
| ≥ 90          | Excellent | Very strong performance     |
| 75–89         | Stable    | Overall good; minor issues  |
| 60–74         | At Risk   | Mixed; some KPIs slipping   |
| < 60          | Critical  | Multiple KPIs in poor state |

Codex must **not** change the mechanism of including only KPIs that have data.

---

## 5. Database Tables Used (Pre-existing Only)

The following tables **already exist** in the database. Codex must **only run
SELECT queries** against them for this feature and must **NOT** generate any
DDL (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`).

### 5.1 `undelivered_orders`

Relevant columns:

```sql
order_id            VARCHAR NOT NULL,
order_date          DATE,
store_code          VARCHAR NOT NULL,
store_name          VARCHAR,
net_amount          DOUBLE PRECISION,
expected_deliver_on DATE,
actual_deliver_on   DATE
```

### 5.2 `missed_leads`

Relevant columns:

```sql
pickup_row_id         INTEGER PRIMARY KEY,
mobile_number         VARCHAR NOT NULL,
customer_name         VARCHAR,
pickup_created_date   DATE,
pickup_created_time   VARCHAR,
pickup_date           DATE,
pickup_time           VARCHAR,
source                VARCHAR,
customer_type         VARCHAR,
is_order_placed       BOOLEAN,
store_code            VARCHAR NOT NULL
```

Any schema evolution for these tables is **explicitly out of scope** for this
specification.

---

## 6. Action Tables (New Sections in the Existing Report)

Two new **action tables** are required:

1. **Undelivered Orders (Action List)**
2. **Missed Leads – Not Converted (Action List)**

They must appear in **two places**:

1. As **regular tables** in the existing HTML/Jinja template for the summary
   PDF (non-interactive, visually writable cells), and
2. As **interactive tables** (with checkboxes + comment boxes) in a new
   **Store Action List PDF** generated with **ReportLab + AcroForm**.

### 6.1 Placement in the Current Summary Report Layout

The simplified current layout of the **summary PDF** is:

1. Header (store name, date, logo, run id, etc.)
2. Overall Health / KPI band summary
3. "Today’s Operating Snapshot" section
4. KPI cards / status sections
5. Recommendations section:

   * Highlights
   * Focus Areas
   * Actions for Today
6. Footer / notes (if any)

Codex must insert the two new tables **after** the Recommendations section and
**before** any footer.

Final visual order in the **summary PDF** must be:

1. Existing content up to and including **Actions for Today**
2. **Undelivered Orders (Action List)** — new table
3. **Missed Leads – Not Converted (Action List)** — new table
4. Any existing footer / notes

No additional pages or separate reports are to be created **inside the summary
PDF**. However, the **interactive Action List PDF** (see Section 13) is a
separate companion PDF.

---

## 7. Undelivered Orders – Table Specification

### 7.1 Filter

When building the data for this table, Codex must use the following filter:

```sql
store_code = {store_code}
AND actual_deliver_on IS NULL
```

`{store_code}` is the store for which the report is being generated.

### 7.2 Ordering

Rows must be ordered by **Age (days) descending** (oldest delays first).

Age (days) is computed as:

```text
age_days = (report_date - expected_deliver_on).days
```

If `expected_deliver_on` is NULL, then Codex must calculate
`expected_deliver_on = order_date + 3`.

### 7.3 Columns & Mappings

The Undelivered Orders table must have **exactly** these columns in both the
HTML summary report and the Action List PDF:

| UI Label                    | Source / Logic                                             | Type                          |
| --------------------------- | ---------------------------------------------------------- | ----------------------------- |
| **Order ID**                | `undelivered_orders.order_id`                              | read-only                     |
| **Order Date**              | `undelivered_orders.order_date`                            | read-only                     |
| **Committed Delivery Date** | `undelivered_orders.expected_deliver_on`                   | read-only                     |
| **Age (days)**              | `report_date - expected_deliver_on` (days)                 | computed, read-only           |
| **Net Amount**              | `undelivered_orders.net_amount`                            | read-only                     |
| **Delivered (Y/N)**         | empty cell for HTML; **checkbox field** in Action List PDF | **Editable** (see Section 13) |
| **Comments**                | empty cell for HTML; **text field** in Action List PDF     | **Editable** (see Section 13) |

At the bottom of this table, `sum(undelivered_orders.net_amount)` must be
printed/displayed aligned with its column in both the summary table and the
Action List PDF.

### 7.4 Data Shape in `store_context`

In `build_store_context(...)`, Codex must add/maintain a key:

```python
context["undelivered_orders_rows"] = undelivered_rows
```

Where `undelivered_rows` is a list of dicts of the form:

```python
{
    "order_id": str,
    "order_date": date | None,
    "committed_date": date | None,
    "age_days": int | None,
    "net_amount": float | None,
}
```

These keys must match what the Jinja template uses and what the Action List
PDF renderer will read.

### 7.5 Jinja Template Example (Summary PDF)

Inside `store_report.html`, after the Recommendations block, Codex must render
this data in a table similar to:

```html
<h2>Undelivered Orders (Action List)</h2>
<table class="action-table action-table-undelivered">
  <thead>
    <tr>
      <th>Order ID</th>
      <th>Order Date</th>
      <th>Committed Delivery Date</th>
      <th>Age (days)</th>
      <th>Net Amount</th>
      <th>Delivered (Y/N)</th>
      <th>Comments</th>
    </tr>
  </thead>
  <tbody>
    {% if undelivered_orders_rows %}
      {% for row in undelivered_orders_rows %}
        <tr>
          <td>{{ row.order_id }}</td>
          <td>{{ row.order_date }}</td>
          <td>{{ row.committed_date }}</td>
          <td>{{ row.age_days }}</td>
          <td>{{ row.net_amount }}</td>
          <td class="cell-input">[   ]</td>
          <td class="cell-input"></td>
        </tr>
      {% endfor %}
    {% else %}
      <tr>
        <td colspan="7">No undelivered orders pending as of this report.</td>
      </tr>
    {% endif %}
  </tbody>
</table>
```

The `[   ]` and blank cells are visual placeholders in the HTML/summary PDF.
The **interactive behaviour** for these columns is implemented **only in the
Action List PDF** using ReportLab + AcroForm (Section 13).

---

## 8. Missed Leads – Not Converted – Table Specification

### 8.1 Filter

For the Missed Leads table, Codex must select rows where the lead has **not
converted into an order** yet:

```sql
store_code = {store_code}
AND is_order_placed = FALSE
```

### 8.2 Ordering

Rows must be ordered as:

```text
customer_type ASC,
pickup_created_date ASC
```

### 8.3 Columns & Mappings

The Missed Leads table must have **exactly** these columns in both the HTML
summary report and the Action List PDF:

| UI Label                       | Source / Logic                                             | Type                          |
| ------------------------------ | ---------------------------------------------------------- | ----------------------------- |
| **Phone**                      | `missed_leads.mobile_number`                               | read-only                     |
| **Customer Name**              | `missed_leads.customer_name`                               | read-only                     |
| **Pickup Created Date / Time** | `pickup_created_date` + `pickup_created_time` (formatted)  | read-only                     |
| **Pickup Time**                | `pickup_date` + `pickup_time` (formatted)                  | read-only                     |
| **Source**                     | `missed_leads.source`                                      | read-only                     |
| **Customer Type**              | `missed_leads.customer_type`                               | read-only                     |
| **Lead Converted**             | empty cell for HTML; **checkbox field** in Action List PDF | **Editable** (see Section 13) |
| **Comments**                   | empty cell for HTML; **text field** in Action List PDF     | **Editable** (see Section 13) |

### 8.4 Data Shape in `store_context`

Codex must add/maintain a key:

```python
context["missed_leads_rows"] = missed_leads_rows
```

Where `missed_leads_rows` is a list of dicts of the form:

```python
{
    "phone": str,
    "customer_name": str | None,
    "pickup_created": str,   # formatted datetime string
    "pickup_time": str | None,
    "source": str | None,
    "customer_type": str | None,
}
```

These keys must match the Jinja template and the Action List PDF renderer.

### 8.5 Jinja Template Example (Summary PDF)

Immediately after the Undelivered Orders table, Codex must render:

```html
<h2>Missed Leads – Not Converted</h2>
<table class="action-table action-table-missed-leads">
  <thead>
    <tr>
      <th>Phone</th>
      <th>Customer Name</th>
      <th>Pickup Created Date / Time</th>
      <th>Pickup Time</th>
      <th>Source</th>
      <th>Customer Type</th>
      <th>Lead Converted</th>
      <th>Comments</th>
    </tr>
  </thead>
  <tbody>
    {% if missed_leads_rows %}
      {% for row in missed_leads_rows %}
        <tr>
          <td>{{ row.phone }}</td>
          <td>{{ row.customer_name }}</td>
          <td>{{ row.pickup_created }}</td>
          <td>{{ row.pickup_time }}</td>
          <td>{{ row.source }}</td>
          <td>{{ row.customer_type }}</td>
          <td class="cell-input">[   ]</td>
          <td class="cell-input"></td>
        </tr>
      {% endfor %}
    {% else %}
      <tr>
        <td colspan="8">No missed leads pending follow-up as of this report.</td>
      </tr>
    {% endif %}
  </tbody>
</table>
```

Again, the interactive behaviour for **Lead Converted** and **Comments** is
implemented only in the Action List PDF.

---

## 9. “Today’s Operating Snapshot” – Delta Colours & Direction

The existing **“Today’s Operating Snapshot”** section shows, for each metric,
current values and a textual change note like:

* "Up by 3.0 compared to the previous report."
* "Down by 1.0 compared to the previous report."
* "Flat versus the previous data point."

Codex must **not** change the `_change_note` logic that generates this text, but
must add **colour cues** based on direction and delta.

### 9.1 Snapshot Metrics and Direction

The snapshot currently includes metrics such as:

| UI Label         | Context Key Example      | Direction |
| ---------------- | ------------------------ | --------- |
| Leads captured   | `leads_total`            | higher    |
| Pickups          | `pickups_total`          | higher    |
| Deliveries       | `deliveries_total`       | higher    |
| New customers    | `new_customers_count`    | higher    |
| Repeat customers | `repeat_customers_count` | higher    |

For now, treat all snapshot metrics as `direction = "higher"`.

### 9.2 Delta Logic (Reference)

Conceptually, the delta is:

```python
if current is None:
    delta = None
elif previous is None:
    delta = None
else:
    delta = float(current) - float(previous)
```

A small threshold (e.g. `abs(delta) <= 0.01`) is treated as **flat** – this
should remain aligned with `_change_note`.

### 9.3 Colour Rules

Codex must apply colour styling to the **Total** value and/or the **Note** text
according to:

#### 9.3.1 For `direction = "higher"` metrics

* If `delta` is significantly **> 0** → performance improved:

  * Apply a **positive (green)** style.
* If `delta` is significantly **< 0** → performance worsened:

  * Apply a **negative (red/orange)** style.
* If `delta` is effectively **0** or `delta` is `None`:

  * Apply a **neutral (black)** style.

#### 9.3.2 For `direction = "lower"` metrics (future extension)

* If `delta` is significantly **< 0** (value decreased) → performance improved:

  * Apply **green**.
* If `delta` is significantly **> 0** (value increased) → performance worsened:

  * Apply **red/orange**.
* If flat or `None` → neutral black.

### 9.4 Recommended CSS Classes

Codex may introduce **three** generic classes for this section:

* `snapshot-positive` → green / improvement
* `snapshot-negative` → red/orange / worsening
* `snapshot-neutral`  → black / flat or no comparison

These classes should be applied consistently; no change to the actual text
content (the "Up/Down/Flat" messages).

---

## 10. Rename “Actions for Tomorrow” → “Actions for Today”

The recommendations block currently uses language like **“Actions for
Tomorrow”**, but operationally:

* The report summarises **yesterday (T)**.
* It is generated early on **today (T+1)**.
* All action items are intended for **today’s operations**.

To reflect this:

### 10.1 Template Text Change

Codex must update the template so that the section heading is:

```html
<h2>Actions for Today</h2>
```

and any wording in that section talks about **today**, not "tomorrow".

### 10.2 Context Key Change

If the recommendations context currently uses a key such as:

```python
"actions_tomorrow": actions,
```

Codex must rename this to:

```python
"actions_today": actions,
```

and update the Jinja template accordingly:

```html
{% for item in actions_today %}
  <li>{{ item }}</li>
{% endfor %}
```

The underlying logic that builds the `actions` list (based on KPI statuses)
should remain unchanged, other than the naming.

---

## 11. Implementation Checklist for Summary Report (HTML + Existing PDF)

* [ ] Do **not** create new KPIs beyond the ones listed here.
* [ ] Modify only the existing `store_report.html` (or equivalent) template.
* [ ] Add the two new tables **after** the Recommendations block.
* [ ] Ensure the order: Undelivered Orders, then Missed Leads.
* [ ] Use the exact column sets and mappings defined in Sections 7 and 8.
* [ ] Extend `build_store_context(...)` to add/maintain `undelivered_orders_rows`
  and `missed_leads_rows` only, with the specified shapes.
* [ ] Use only `SELECT` queries against `undelivered_orders` and `missed_leads`.
* [ ] Do not alter database schema.
* [ ] Implement snapshot colour rules without changing `_change_note` text.
* [ ] Rename “Actions for Tomorrow” to “Actions for Today” in both data and
  template.

---

## 12. New Interactive Store Action List PDF (ReportLab + AcroForm)

In addition to the existing summary PDF, Codex must implement a new companion
PDF that contains **only** the two action tables, with **interactive PDF form
fields** using **ReportLab + AcroForm**.

### 12.1 Output File

* Suggested filename pattern (can be wired in orchestrator):

  ```text
  {store_code}_{report_date}_action_list.pdf
  ```

* The orchestrator should generate this file alongside the summary PDF and
  attach/ship both wherever the current pipeline sends reports.

### 12.2 Data Source

The Action List PDF must be built **entirely** from the existing
`build_store_context(...)` output:

* `context["undelivered_orders_rows"]`
* `context["missed_leads_rows"]`
* `context["store_name"]`, `context["report_date"]`, `context["run_id"]`, etc.

No additional queries beyond those defined earlier are required.

### 12.3 Layout Requirements

On the Action List PDF:

1. A simple header (store name, report date, run id).
2. A section:

   * Title: **Undelivered Orders (Action List)**
   * Table using the columns from Section 7.3
   * Checkboxes for **Delivered (Y/N)** and textfields for **Comments**.
3. A section:

   * Title: **Missed Leads – Not Converted**
   * Table using the columns from Section 8.3
   * Checkboxes for **Lead Converted** and textfields for **Comments**.

Exact fonts and colours can be basic (e.g. Helvetica, simple lines). Focus is on
**correct data mapping** and **working form fields**.

### 12.4 ReportLab + AcroForm Implementation

Codex must use **ReportLab**’s low-level canvas with AcroForm. A typical pattern
for rendering one table with interactive cells is as follows (illustrative only;
Codex must adapt to actual coordinates and field sizes):

```python
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.pdfgen import canvas


def build_action_list_pdf(output_path: str, store_context: dict) -> None:
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4

    form = c.acroForm

    # --- Header ---
    c.setFont("Helvetica-Bold", 14)
    title = f"Store Action List — {store_context['store_name']}"
    c.drawString(50, height - 40, title)

    c.setFont("Helvetica", 9)
    c.drawString(50, height - 55, f"Report date: {store_context['report_date']}")
    c.drawString(50, height - 68, f"Run ID: {store_context['run_id']}")

    y = height - 100

    # --- Undelivered Orders (Action List) ---
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Undelivered Orders (Action List)")
    y -= 20

    # Table headers (example x positions)
    x_order_id = 50
    x_order_date = 120
    x_committed = 190
    x_age = 270
    x_amount = 320
    x_delivered = 390
    x_comment = 440

    headers = [
        (x_order_id, "Order ID"),
        (x_order_date, "Order Date"),
        (x_committed, "Committed Delivery Date"),
        (x_age, "Age"),
        (x_amount, "Net Amt"),
        (x_delivered, "Delivered (Y/N)"),
        (x_comment, "Comments"),
    ]

    c.setFont("Helvetica-Bold", 9)
    for x, label in headers:
        c.drawString(x, y, label)

    y -= 16
    c.setLineWidth(0.5)
    c.line(50, y, width - 50, y)
    y -= 10

    c.setFont("Helvetica", 8)

    # Optional: constant for read-only textfields if you choose to use them
    READ_ONLY_FLAG = 1

    for idx, row in enumerate(store_context.get("undelivered_orders_rows", []), start=1):
        if y < 60:  # simple new-page check
            c.showPage()
            y = height - 60
            # (Re-draw headers if needed)

        order_id = row.get("order_id") or ""
        order_date = row.get("order_date") or ""
        committed = row.get("committed_date") or ""
        age_days = row.get("age_days")
        net_amount = row.get("net_amount")

        # Draw read-only values as plain text
        c.drawString(x_order_id, y, str(order_id))
        c.drawString(x_order_date, y, str(order_date))
        c.drawString(x_committed, y, str(committed))
        c.drawString(x_age, y, str(age_days) if age_days is not None else "")
        c.drawRightString(x_amount + 40, y, f"{net_amount:.2f}" if net_amount is not None else "")

        # Editable checkbox for Delivered (Y/N)
        form.checkbox(
            name=f"undelivered_delivered_{idx}",
            tooltip="Delivered?",
            x=x_delivered + 4,
            y=y - 2,
            size=10,
            borderColor=colors.black,
            fillColor=colors.white,
            buttonStyle="check",
            borderWidth=1,
        )

        # Editable comments textbox
        form.textfield(
            name=f"undelivered_comment_{idx}",
            tooltip="Comments",
            x=x_comment,
            y=y - 3,
            width=140,
            height=12,
            borderWidth=1,
            borderColor=colors.black,
            textColor=colors.black,
        )

        y -= 18

    # --- Similar pattern for Missed Leads – Not Converted ---
    # Use context['missed_leads_rows'] and field names like
    # "missed_lead_converted_{idx}" and "missed_lead_comment_{idx}".

    c.showPage()
    c.save()
```

**Key points Codex must follow:**

* Use `canvas.Canvas(..., pagesize=A4)`.
* Use `form = c.acroForm`.
* For **editable** columns:

  * Use `form.checkbox(...)` for boolean/tick fields.
  * Use `form.textfield(...)` for comments.
* Use **unique field names** per row and per table, for example:

  * `undelivered_delivered_{idx}` and `undelivered_comment_{idx}`
  * `missed_lead_converted_{idx}` and `missed_lead_comment_{idx}`
* For read-only columns, simple `drawString` is sufficient. No need to create
  form fields for them.

### 12.5 Integration With Existing Codebase

Codex must:

1. Add a function similar to `build_action_list_pdf(output_path: str, store_context: dict)`
   in an appropriate module (e.g. alongside the existing report generator).
2. Ensure it is called from the same orchestration path that currently builds
   the summary PDF, using the existing `store_context`.
3. Ensure the new PDF is generated **in addition to** the summary PDF and is
   included wherever PDFs are currently stored/emailed.

There is **no requirement** to switch the main summary PDF from Playwright to
ReportLab at this time.

---

## 13. Final Implementation Checklist (Action List PDF)

* [ ] Implement a new Action List PDF using **ReportLab + AcroForm**.
* [ ] Use `undelivered_orders_rows` and `missed_leads_rows` from
  `build_store_context(...)`.
* [ ] Render two sections: Undelivered Orders (Action List) and Missed Leads –
  Not Converted.
* [ ] For each row:

  * [ ] Draw all **read-only** values as plain text.
  * [ ] Add a checkbox field for the boolean column (Delivered / Lead Converted).
  * [ ] Add a textfield for Comments.
* [ ] Ensure field names are unique and follow a consistent naming scheme.
* [ ] Generate the PDF alongside the existing summary PDF and keep the
  orchestrator behaviour otherwise unchanged.

---

## 14. End of Specification

This is the final, authoritative, version-controlled specification for
**TSV Store Performance Report v2.4** (including KPI bands, action tables,
snapshot colours, corrected action terminology, and the new interactive
Store Action List PDF using ReportLab + AcroForm).
