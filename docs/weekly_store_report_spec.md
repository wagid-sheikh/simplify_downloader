# Weekly Store Performance Report — Specification (SSOT v1)

## 1. Purpose
The Weekly Store Performance Report summarizes **last week’s** operational performance (Monday–Sunday) and provides **actionable insights for this week**. It is not an interactive PDF. No editable fields, no checkboxes. All content is static and display-only.

The weekly report is generated after the full week closes.

## 2. Inputs & Data Sources
The report must use:
- Aggregated KPI & operational data from the existing ingestion pipeline.
- Date range: last Monday–Sunday.
- Undelivered Orders backlog **as of last Sunday**.
- Missed Leads (open, not converted) **as of last Sunday**.

## 3. Output Format
- PDF file rendered via the existing HTML → Playwright PDF pipeline.
- Uses its own HTML template: `weekly_report.html`
- Must *not* display **store code** anywhere.

## 4. Sections & Layout Requirements

### 4.1 Header
Display the following:
- Store Name (large heading)
- Period: “Last Week: {start_date} – {end_date}”
- Generated At (datetime ISO format)
- Run ID
- Logo (if provided)

### 4.2 Weekly Overall Health (Last Week)
A visual summary block containing:
- **Weekly Health Score** (0–100)
- Label: Excellent / Stable / At Risk / Critical
- Summary narrative (2–3 sentences)

#### Weekly Metrics Strip:
One-line compact metrics:
- Total revenue (last week)
- Total pickups
- Total deliveries
- Total new customers
- Total repeat customers

### 4.3 Weekly KPI Overview Table
A KPI table with columns:
- KPI
- Weekly Average
- Best Day (value + date)
- Worst Day (value + date)
- Weekly Band (Excellent / Good / Needs Attention / Critical)

KPIs:
- Pickup conversion (total %)
- Pickup conversion (new %)
- Pickup conversion (existing %)
- Delivery within TAT (%)
- Undelivered >10 days (count)
- Total undelivered (count)
- Repeat customer contribution (%)

### 4.4 Weekly Volume & Mix Snapshot

#### Table A: Weekly Totals
For the full last week:
- Leads captured
- Pickups
- Deliveries
- New customers
- Repeat customers
- High-value orders (>₹800)
- Average revenue/day
- Peak revenue day (value + date)

#### Table B: Day-by-Day Breakdown
Columns:
- Day (Mon–Sun)
- Leads
- Pickups
- Deliveries
- Revenue

### 4.5 Weekly Insights (Last Week) & Focus for This Week
Three titled blocks:

#### A. Highlights (Last Week)
3–5 bullet points summarizing best performance aspects.

#### B. Risk Areas (Last Week)
3–5 bullet points identifying weak areas.

#### C. Focus for This Week
Clear action items based on last week’s performance.

### 4.6 Undelivered Orders — Backlog (as of Last Week End)
Heading:  
“Undelivered Orders — Backlog as of {end_date}”

Table columns:
- Order ID
- Order Date
- Committed Delivery Date
- Age (days)
- Net Amount
- Delivered (Y/N) → blank
- Comments → blank

Total Net Amount row at bottom.

### 4.7 Missed Leads — Not Converted (Backlog as of Last Week End)
Heading:
“Missed Leads — Not Converted (Open as of {end_date})”

Table columns:
- Phone
- Customer Name
- Pickup Created Date/Time
- Pickup Time
- Source
- Customer Type
- Lead Converted → blank
- Comments → blank

### 4.8 Footer
`TSV Weekly Store Performance Report • Week: {start_date} – {end_date}`

## 5. Constraints
- Do **not** display store code.
- No editable fields (no AcroForms).
- PDF must follow the same typography & styling as daily report.
- HTML template name: `weekly_report.html`.
