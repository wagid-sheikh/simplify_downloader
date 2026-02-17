# UC Orders Sync Runbook (API-primary)

## Local run

Use the standard runner:

```bash
./scripts/run_local_uc_orders_sync.sh --from-date 2026-02-01 --to-date 2026-02-15
```

## Runtime controls

  - Set to `1` only during rollback windows when GST API output is unavailable.
- `UC_ARCHIVE_EXTRACTION_MODE` (default: `api`): archive extraction mode (`api`, `ui`, `api_with_ui_fallback`).

Example rollback invocation:

```bash
./scripts/run_local_uc_orders_sync.sh --from-date 2026-02-01 --to-date 2026-02-15
```
