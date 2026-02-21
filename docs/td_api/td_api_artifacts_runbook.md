# TD API artifacts runbook (`td_orders_sync`)

Use this runbook to force TD API execution modes and verify API artifacts/logging.

## Required runtime mode

Run `td_orders_sync` with one of the API-capable modes:

- `--source-mode api_shadow` (UI remains source of truth, API is fetched + compared)
- `--source-mode api_primary` (API is preferred source when available)
- `--source-mode api_only` (API-only flow)

If you leave source mode as `ui`, TD API fetch/compare artifacts are not expected.

## Artifact directory behavior

TD API artifacts are written to:

1. `TD_API_ARTIFACT_DIR` if provided.
2. Otherwise, the default CRM download directory (`app/crm_downloader/data` via `default_download_dir()`).

Example override:

```bash
export TD_API_ARTIFACT_DIR="/tmp/td-api-artifacts"
```

## Invocation examples

Run through helper script:

```bash
scripts/run_local_td_orders_sync.sh --source-mode api_shadow --stores A817
```

Or run module directly:

```bash
poetry run python -m app.crm_downloader.td_orders_sync.main --source-mode api_shadow --stores A817
```



## API↔UI parity mapping reference (orders + sales)

Use this table as the shared parity contract for tests and incident triage.

| Dataset | API field | Canonical/UI-equivalent field | Contract |
| --- | --- | --- | --- |
| Orders | `orderNo` | `order_number` | Required mapped field |
| Orders | `orderDate` | `order_date` | Required mapped field |
| Orders | `amount` | `amount` | Required mapped field |
| Orders | `status` | `status` | Required mapped field |
| Orders | `bookingSlipUrl` | n/a | Allowed API-only field |
| Orders | `storeName` | n/a | Allowed API-only field |
| Orders | `deliveryDate` | n/a | Allowed API-only field |
| Sales | `orderNo` | `order_number` | Required mapped field |
| Sales | `paymentDate` | `payment_date` | Required mapped field |
| Sales | `paymentMode` | `payment_mode` | Required mapped field |
| Sales | `amount` | `amount` | Required mapped field |
| Sales | `status` | `status` | Required mapped field |
| Sales | `printer` | n/a | Allowed API-only field |
| Sales | `storeName` | n/a | Allowed API-only field |
| Sales | `storeId` | n/a | Allowed API-only field |

## Summary/footer row filtering rules (orders + sales)

To avoid compare noise from aggregate rows returned by TD APIs, the downloader filters summary/footer rows from `orders_rows` and `sales_rows` immediately after `_extract_rows(...)`.

A row is filtered when either of these heuristics matches:

1. `orderNumber`/`orderNo` is empty (null/blank) **and** a label-like field (`label`, `name`, `title`, `description`, `remark`, `note`, `particular`) contains summary markers such as `Total`, `Summary`, or `Grand Total`.
2. The row contains summary markers and aggregate-only numeric values (for example totals/tax amounts) but no stable transaction identifier (`orderNumber`, `orderNo`, `orderId`, `transactionId`, `invoiceNo`, `receiptNo`, `paymentId`).

Operational visibility:

- Each endpoint logs `summary_rows_filtered` via `TD API summary rows filtered` with endpoint and store code context.

## Expected observability signals

In logs, verify API phase events are present for the store:

- `message="Prepared API client from per-store session artifact"`
- `message="Persisted TD API artifacts"`
- `phase="api"`
- `artifact_dir` points to your resolved output directory

## Troubleshooting: no TD API Excel/artifact files generated

1. Confirm run used API mode (`--source-mode api_shadow|api_primary|api_only`).
2. Confirm run reached TD flow (store is TD-enabled and login/OTP passed).
3. Confirm `artifact_dir` appears in `Persisted TD API artifacts` log entry.
4. Check `TD_API_ARTIFACT_DIR` for typos and write permissions.
5. Capture for debugging:
   - full command
   - `TD_API_ARTIFACT_DIR` value
   - `source_mode` value
   - log lines around `Prepared API client` and `Persisted TD API artifacts`
