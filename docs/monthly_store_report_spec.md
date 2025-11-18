# Monthly Store Performance Report — Specification (SSOT v1)

## 1. Purpose
The Monthly Store Performance Report summarizes **last month’s** performance and identifies improvement areas for **this month**. It is a static PDF (no editable fields).

## 2. Inputs & Data Sources
The report must use:
- Aggregated KPI & operational data for last calendar month (1–30/31).
- Undelivered Orders backlog **as of last day of last month**.
- Missed Leads not converted **as of last day of last month**.

## 3. Output Format
- PDF file rendered via existing HTML → Playwright PDF pipeline.
- Uses unique template: `monthly_report.html`
- Must *not* display store code anywhere.

## 4. Sections & Layout Requirements

### 4.1 Header
- Store Name (large title)
- “Last Month: {MonthName YYYY}”
- Generated At (datetime ISO format)
- Run ID
- Logo (if provided)

### 4.2 Monthly Overall Health (Last Month)
Visual summary block:
- **Monthly Health Score** (0–100)
- Label: Excellent / Stable / At Risk / Critical
- Summary narrative (2–3 sentences)

#### Monthly Metrics Strip:
- Total monthly revenue
- Total pickups
- Total deliveries
- Total new customers
- Total repeat customers

### 4.3 Monthly KPI Performance Table
Columns:
- KPI  
- Monthly Average  
- Best Day (value + date)  
- Worst Day (value + date)  
- Monthly Band  

KPIs:
- Pickup conversion (total %)
- Pickup conversion (new %)
- Pickup conversion (existing %)
- Delivery within TAT (%)
- Undelivered >10 days (count)
- Total undelivered (count)
- Repeat customer contribution (%)

### 4.4 Week-by-Week Breakdown (Inside the Month)
Table with one row per week of the month:
Columns:
- Week Label (e.g., Week 1: 1–7 Nov)
- Revenue
- Pickups
- Deliveries
- Avg Conversion (%)
- Avg TAT (%)

### 4.5 Monthly Volume & Mix Snapshot
Table of last month’s totals:
- Leads captured
- Pickups
- Deliveries
- New customers
- Repeat customers
- High-value orders
- Avg revenue/day
- Peak revenue day (value + date)

Optional:
Day-of-week pattern table (averaged across the month).

### 4.6 Monthly Insights & Focus for This Month
Three blocks:

#### A. What Worked Last Month
3–5 bullets.

#### B. Where We Lost Value
3–5 bullets.

#### C. Focus for This Month
3–6 actionable goals.

### 4.7 Undelivered Orders — Backlog (as of Last Month End)
Heading:
“Undelivered Orders — Backlog as of {last_month_end}”

Table columns:
- Order ID
- Order Date
- Committed Delivery Date
- Age (days)
- Net Amount
- Delivered (Y/N) → blank
- Comments → blank

Bottom row: Total Net Amount.

### 4.8 Missed Leads — Not Converted (as of Last Month End)
Heading:
“Missed Leads — Not Converted (Open as of {last_month_end})”

Columns:
- Phone
- Customer Name
- Pickup Created Date/Time
- Pickup Time
- Source
- Customer Type
- Lead Converted → blank
- Comments → blank

### 4.9 Footer
`TSV Monthly Store Performance Report • Month: {MonthName YYYY}`

## 5. Constraints
- Do **not** display store code.
- No editable fields.
- Must follow daily report’s styling.
- Template name must be: `monthly_report.html`.
