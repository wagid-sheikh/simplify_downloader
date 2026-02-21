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
2. Otherwise, `docs/td_api/artifacts`.

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
