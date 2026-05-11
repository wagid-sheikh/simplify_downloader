# Net/Gross Amount Refactor to `order_amount` — Audit Context

## Purpose of this document

This document captures the agreed business rules and intended architecture for the completed refactor from inconsistent raw order amount usage to the canonical `vw_orders.order_amount` contract.

Use this file as copy/paste context for a future Codex audit session. The audit goal is to verify that the implementation is complete, consistent, and protected against future regressions.

---

## Background

The repo previously used these `orders` columns inconsistently across reports and views:

- `orders.net_amount`
- `orders.gross_amount`
- `orders.source_system`
- `orders.adjustment`

The problem was that different reports and SQL views selected order value in different ways. Some logic used `net_amount`, some used `gross_amount`, some applied source-system-specific logic, and some ignored adjustment behavior.

The intended fix was to introduce one canonical reporting and decision-making amount:

```text
vw_orders.order_amount
```

The raw source fields remain valid for ingest/sync purposes, but reports and payment/recovery decisions must use `order_amount`.

---

## Confirmed business rules

### Canonical order amount

The standardized order value is named:

```text
order_amount
```

It is exposed through the database view:

```text
vw_orders
```

The base table remains:

```text
orders
```

The raw fields remain present because ingest/sync still need them:

- `orders.net_amount`
- `orders.gross_amount`
- `orders.adjustment`
- `orders.source_system`

However, these raw fields must not drive reports, payment checks, pending-payment checks, recovery checks, pending-delivery decision logic, or any other business decision-making path.

---

### Source-system amount rule

For every order, choose the base amount as follows.

#### If `source_system = 'TumbleDry'`

Use `net_amount` only when it is non-null and non-zero.

If `net_amount` is null or zero, fallback to `gross_amount`.

If both values are null or zero, the base amount becomes zero.

#### If `source_system != 'TumbleDry'`

Use `gross_amount`.

This includes:

- `UClean`
- any unknown future source system

Production source-system values are expected to be:

```text
TumbleDry
UClean
```

Unknown source systems should default to `gross_amount`.

---

### Adjustment rule

`orders.adjustment` applies to all source systems, not only TumbleDry.

Confirmed interpretation of `adjustment`:

- It is a discount or reduction to order value.
- It represents a manually corrected value.
- It exists when the original order amount was wrong.
- It exists when there is an amount conflict with the customer.
- It is manually passed to align the amount.

Rules:

```text
adjustment IS NULL => 0
adjustment <= 0    => no reduction
adjustment > 0     => reduce base amount
```

Formula:

```text
final_amount = base_amount - adjustment
```

If `final_amount <= 0`, then:

```text
order_amount = 0
```

`order_amount` must never be negative.

---

### Zero-value order behavior

If standardized `order_amount` becomes zero:

- Include the order in descriptive/order-listing contexts where appropriate.
- Exclude the order from:
  - missing-payment checks;
  - pending-payment checks;
  - payment-recovery/action-required sections;
  - pending-payment amount-at-risk logic.

The key distinction is that zero-value orders may be visible as orders, but they must not create payment/recovery action items.

---

### Payment tolerance rule

Paid-in-full checks must use built-in tolerance:

```text
tolerance = 1
```

Canonical comparison:

```text
paid_amount + 1 >= order_amount
```

Overpayments are always considered paid in full.

Examples:

```text
order_amount = 1000
paid_amount = 999
=> paid in full because 999 + 1 >= 1000

order_amount = 1000
paid_amount = 1000
=> paid in full

order_amount = 1000
paid_amount = 1001
=> paid in full
```

---

## Intended architecture after the completed PRs

### 1. Canonical view: `vw_orders`

There should be a forward-only Alembic migration creating or replacing:

```text
vw_orders
```

Expected properties:

- Exposes all original `orders` columns unchanged.
- Adds derived column `order_amount`.
- Implements source-system fallback, adjustment handling, and zero-floor behavior in one place.
- Contains comments or nearby documentation explaining the raw-vs-reporting field contract.

Expected high-level logic:

```sql
CASE
  WHEN final_amount <= 0 THEN 0
  ELSE final_amount
END
```

Where:

1. base amount follows source-system rules;
2. positive adjustment reduces the amount;
3. final result is floored at zero.

Historical Alembic migrations should not have been modified. The view should have been added or updated through forward-only migration(s).

---

### 2. Existing missing-payment view refactored

The existing view:

```text
vw_orders_missing_in_payment_collections
```

should now depend on:

```text
vw_orders.order_amount
```

It should no longer directly implement source-specific raw amount logic using `orders.net_amount` or `orders.gross_amount`.

Expected behavior:

- Reads from `vw_orders`.
- Uses `order_amount` for payment comparison.
- Applies paid-in-full tolerance:

```text
paid_amount + 1 >= order_amount
```

- Excludes zero-value orders from missing-payment/action-required results:

```text
order_amount > 0
```

---

### 3. Report reads routed through `vw_orders`

All reporting and business-decision reads that previously referenced `orders` directly should now read through:

```text
vw_orders
```

Important report areas:

- Daily sales report
- Missing-payment report
- MTD missing-payment report
- Same-day fulfillment report
- MTD same-day fulfillment report
- Pending deliveries report
- To-be-recovered / to-be-compensated report
- Dashboard PDF/report generator paths

Priority paths originally identified:

```text
app/reports/daily_sales_report/data.py
app/reports/daily_sales_report/to_be_recovered.py
app/reports/pending_deliveries/data.py
app/reports/shared/same_day_fulfillment.py
app/reports/mtd_same_day_fulfillment/data.py
app/dashboard_downloader/
```

Expected outcome:

- Report code should use `vw_orders.order_amount`.
- Report code should not use raw `net_amount`, `gross_amount`, or `adjustment` for amount decisions.
- Raw fields may exist in `vw_orders` and ingest/sync code, but should not drive report decisions.

---

### 4. Daily sales report semantics

The daily sales report has two separate concepts.

#### Order-side sales-done columns

These come from order records.

Expected source:

```text
vw_orders.order_amount
```

User-facing label:

```text
Order Amount
```

#### Collection-side columns

These come from actual payment/collection data, such as the `sales` table.

Expected source:

```text
sales
```

Important: collection values should not be replaced with `order_amount`.

The audit should verify that the implementation preserves this distinction:

```text
orders/sales done != collections received
```

For edited-order loss or similar comparisons, logic should compare standardized order amount against the relevant edited/new amount, not raw `net_amount` or `gross_amount`.

---

### 5. Pending deliveries behavior

Pending deliveries should use:

```text
vw_orders.order_amount
```

for:

- order value;
- pending amount;
- amount at risk;
- aging bucket totals;
- detail rows where order value is displayed.

Expected pending amount formula:

```text
max(order_amount - paid_amount, 0)
```

SQL may use `GREATEST(...)` or an equivalent `CASE` expression.

Zero-value orders should be excluded from payment-action, pending-payment, and amount-at-risk sections.

Existing recovery status exclusion contract should remain intact. These statuses should be excluded from pending-delivery aging buckets/details:

```text
TO_BE_RECOVERED
TO_BE_COMPENSATED
RECOVERED
COMPENSATED
WRITE_OFF
```

---

### 6. Recovery and compensation reports

Recovery and compensation report logic should use:

```text
vw_orders.order_amount
```

Expected affected areas include:

```text
app/reports/daily_sales_report/to_be_recovered.py
```

and any related daily-sales report sections/templates.

Expected behavior:

- Display standardized order value.
- Use user-facing label `Order Amount`.
- Exclude zero-value orders from payment-recovery/action-required sections.
- Do not make recovery decisions from raw `net_amount` or `gross_amount`.

---

### 7. Same-day fulfillment reports

Same-day fulfillment reports previously used fields named like:

```text
net_amount
```

Expected new behavior:

- Use `vw_orders.order_amount`.
- Report-facing DTO/dataclass/template fields should be renamed to `order_amount` where practical.
- User-facing label should be `Order Amount`.
- Raw `net_amount` should remain only in ingest/sync/raw-source contexts.

Affected paths likely include:

```text
app/reports/shared/same_day_fulfillment.py
app/reports/mtd_same_day_fulfillment/data.py
```

---

### 8. Guard tests against direct report access to `orders`

There should be static regression protection that prevents report code from accidentally reading the base `orders` table.

Expected test module may be something like:

```text
tests/test_orders_view_guard.py
```

Expected guarded paths:

```text
app/reports/
app/dashboard_downloader/
```

Expected forbidden patterns include:

```text
FROM orders
JOIN orders
sa.table("orders", ...)
Table("orders", ...)
```

and direct report decision usage of:

```text
net_amount
gross_amount
adjustment
```

Allowed cases:

- references to `vw_orders`;
- tests creating fixture tables;
- Alembic migrations;
- ingest/sync code, especially under `app/crm_downloader/`;
- raw source synchronization paths that are not reporting/decision-making.

Expected failure message should make the contract clear:

```text
Reports and decision-making code must use vw_orders.order_amount.
Raw orders.net_amount, orders.gross_amount, and orders.adjustment are source/ingest fields only.
```

The audit should verify that this guard is neither too weak nor too broad.

---

### 9. Tests updated for standardized semantics

Existing tests should validate `vw_orders.order_amount` behavior.

Expected test coverage should include:

1. `TumbleDry` with positive `net_amount` uses `net_amount`.
2. `TumbleDry` with null `net_amount` falls back to `gross_amount`.
3. `TumbleDry` with zero `net_amount` falls back to `gross_amount`.
4. `UClean` uses `gross_amount`.
5. Unknown source system uses `gross_amount`.
6. Positive `adjustment` reduces amount.
7. Null adjustment is treated as zero.
8. Zero or negative adjustment does not reduce amount.
9. Final amount less than or equal to zero becomes zero.
10. Zero-value orders are excluded from missing-payment checks.
11. Zero-value orders are excluded from pending-payment checks.
12. Paid amount within tolerance `1` is treated as paid in full.
13. Overpayment is treated as paid in full.
14. Report tests use `vw_orders` rather than direct `orders` amount fields.

Affected tests likely include:

```text
tests/test_daily_sales_report_data.py
tests/test_pending_deliveries_data.py
tests/test_same_day_fulfillment_shared.py
tests/test_mtd_same_day_fulfillment.py
```

and any missing-payment/recovery tests.

---

### 10. Documentation updates

Canonical docs should mention `vw_orders.order_amount` as the reporting and decision-making contract.

Expected updated docs:

```text
AGENTS.md
docs/architecture.md
docs/decision-log.md
docs/pr-checklist.md
docs/feature-map.md
```

Expected documented rules:

- `vw_orders.order_amount` is mandatory for reports and decision-making.
- Direct report access to `orders` is prohibited unless explicitly approved.
- Raw `net_amount`, `gross_amount`, and `adjustment` are source/ingest fields.
- Payment tolerance is `1`.
- Overpayments are paid in full.
- Zero-value orders are excluded from missing-payment, pending-payment, and recovery action checks.
- User-facing reports should say `Order Amount`.
- Ingest/sync code may still use raw columns when the purpose is synchronization.

---

### 11. User-facing labels

Reports should no longer show misleading labels like:

```text
Net Amount
```

when the value is actually standardized `order_amount`.

Expected label:

```text
Order Amount
```

This applies to:

- PDFs;
- report tables;
- CSV exports;
- email templates;
- dashboard-generated report outputs;
- same-day fulfillment sections;
- pending deliveries;
- recovery/compensation sections;
- daily sales order-side columns.

Raw source diagnostics may still mention `net_amount`, `gross_amount`, or `adjustment` if those are genuinely raw diagnostic values.

---

## Suggested audit prompt for the next Codex session

Copy and paste this prompt into the next session:

```text
Perform a full deep static audit of this repo after the order amount standardization work.

Primary goal:
Verify that the implementation fully and correctly standardizes order monetary logic through vw_orders.order_amount.

Audit these requirements:

1. vw_orders exists via forward-only Alembic migration and exposes all orders columns plus order_amount.
2. order_amount follows:
   - TumbleDry uses net_amount when non-null/non-zero, otherwise gross_amount.
   - UClean and unknown source_system use gross_amount.
   - null base amount becomes zero.
   - null adjustment is zero.
   - only positive adjustment reduces amount.
   - final amount <= 0 becomes zero.
3. Historical migrations were not modified.
4. vw_orders_missing_in_payment_collections uses vw_orders.order_amount and no duplicated raw amount/source logic.
5. All report and decision-making reads use vw_orders instead of orders.
6. Raw net_amount/gross_amount/adjustment remain allowed only for ingest/sync/raw-source purposes.
7. Payment checks use paid_amount + 1 >= order_amount.
8. Overpayments are paid in full.
9. Zero-value orders are excluded from missing-payment, pending-payment, and recovery/action-required sections.
10. Pending deliveries use order_amount for order value, pending amount, amount at risk, aging buckets, and detail rows.
11. Daily sales preserves the distinction between order-side sales done from vw_orders.order_amount and collection-side values from sales/payment data.
12. Recovery/compensation reports use order_amount.
13. Same-day fulfillment reports use order_amount and no longer expose misleading report-facing net_amount naming.
14. User-facing labels say Order Amount where standardized amount is shown.
15. Static guard tests prevent future direct report access to orders and raw amount decision fields.
16. Existing tests cover source-system fallback, adjustment, zero floor, tolerance, overpayment, and zero-value exclusion.
17. Documentation establishes vw_orders.order_amount as the durable reporting contract.

This is a read-only QA audit. Do not modify files. For every issue found, provide a task-stub immediately after the issue using the required task-stub format.
```

---

## Specific things the next audit should watch for

### 1. TumbleDry fallback implemented incorrectly

Incorrect:

```text
TumbleDry always uses net_amount, even when net_amount = 0
```

Correct:

```text
TumbleDry uses net_amount only when non-null and non-zero; otherwise gross_amount
```

---

### 2. Adjustment applied before fallback incorrectly

Correct sequence:

1. choose base amount;
2. apply positive adjustment;
3. floor result at zero.

---

### 3. Adjustment only applied to TumbleDry

Incorrect if adjustment is only applied when:

```text
source_system = 'TumbleDry'
```

Correct:

```text
apply positive adjustment for all source systems
```

---

### 4. Missing-payment logic still duplicates old amount CASE

The missing-payment view should not contain a separate source-specific amount `CASE` expression unless it is simply inherited from `vw_orders`.

Suspicious duplicated logic may look like:

```sql
CASE WHEN source_system = 'TumbleDry' THEN net_amount ELSE gross_amount END
```

---

### 5. Tolerance applied backwards

Correct:

```text
paid_amount + 1 >= order_amount
```

Risky or incorrect:

```text
paid_amount >= order_amount + 1
```

or exact equality checks.

---

### 6. Zero-value orders excluded too broadly

Zero-value orders should be excluded from payment-action logic, but not necessarily erased from every descriptive order report.

The audit should check whether the implementation accidentally removes zero-value orders from all reporting contexts.

---

### 7. Daily sales collections accidentally changed

Order-side sales values should use `vw_orders.order_amount`.

Collection-side values should remain actual collections/payments.

The audit should ensure collection metrics were not replaced with order values.

---

### 8. Static guard too weak

The guard should catch direct use of the base table `orders` in report code, including:

```sql
FROM orders
JOIN orders
```

and SQLAlchemy table definitions such as:

```text
sa.table("orders", ...)
Table("orders", ...)
```

It should not merely search for `orders.` because raw strings and aliases can bypass that.

---

### 9. Static guard too broad

The guard should not fail legitimate ingest/sync code.

Allowed raw use cases include:

```text
app/crm_downloader/
ingest/sync modules
Alembic migrations
tests creating fixture tables
```

---

### 10. Naming changed in code but not templates

DTOs may be renamed to `order_amount`, but templates/PDFs/emails may still say `Net Amount`.

The audit should inspect templates and generated report column labels.

---

## Checks from the context-summary creation session

This document was created as a durable markdown file so it can be copied or referenced easily in future audit sessions.
