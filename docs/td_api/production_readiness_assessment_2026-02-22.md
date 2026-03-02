# TD Orders Sync (`api_shadow`) Production Readiness Assessment

## Inputs reviewed
- Run log: `temp_run_time_stuff/run_log.txt`
- Browser HAR: `docs/td_api/subs.quickdrycleaning.com.har`

## Key findings
1. **Both stores required forced re-login** because persisted storage state was invalid and redirected to `/Login`.
2. **Ingest completed but with data quality warnings** (invalid phone number fallback applied in both orders and sales ingestion).
3. **API compare parity looked good** (no missing rows/mismatches in shadow compare metrics), but the request metadata shows repeated 401s before fallback.
4. **Auth shape mismatch vs HAR is real**:
   - HAR `garments/details` browser requests do not show token query/auth headers.
   - Runtime metadata in shadow run shows `har_like` attempts returning 401, then fallback to legacy auth shape with token query + authorization context returning 200.
5. **Reconciliation still marked both stores as failed** (`passed_stores: 0`, `failed_stores: 2`) despite per-store `window_summary` final status success.

## Interpretation
- The pipeline currently succeeds in a **resilient/fallback** mode, not in a clean primary-path mode.
- The HAR suggests the browser-authenticated shape differs from what backend calls need in this runtime context (or HAR capture omitted sensitive auth artifacts). Either way, this is operationally fragile.
- Reconciliation failing both stores is a direct production gating signal and should be resolved before promoting.

## Production readiness verdict
**Not ready for production yet**.

## Recommended pre-production checklist
1. Make reconciliation pass criteria explicit and ensure this exact run shape produces `passed_stores == total_stores`.
2. Reduce/remove repeated 401-first behavior (verify token source/refresh timing and default auth shape selection).
3. Decide policy for invalid phone fallback warnings (acceptable warning vs blocking data quality issue).
4. Run at least 3 consecutive scheduled-like shadow runs with:
   - zero unexpected auth fallbacks,
   - stable compare parity,
   - reconciliation fully passing.
