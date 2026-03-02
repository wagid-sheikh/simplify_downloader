# TD orders sync readiness review (run_id: 20260302_160221584828)

## Inputs reviewed
- Runtime log: `temp_run_time_stuff/run_log.txt`
- HAR capture: `docs/td_api/subs.quickdrycleaning.com.har`

## What the latest run shows
- The run completed end-to-end with `overall_status: success`.  
- Both stores (`A817`, `A668`) completed UI download, API fetch, compare, and window summary steps with `final_status: success` at the store/window level.
- However, the reconciliation output says **`passed_stores: 0` and `failed_stores: 2`**, with the message explicitly mentioning shadow readiness and promotion gating.

## Warnings observed (non-fatal)
- Session probe warning for both stores: cached storage state redirected to `/Login`, requiring re-login before continuing.
- Ingestion warnings for both stores due to invalid phone number fallback normalization (1-2 records per workbook).

## UI/API comparison quality
- Compare phases reported all key metrics matched for both stores (`missing_in_api: 0`, `missing_in_ui: 0`, amount/status mismatches: `0`) and compare artifacts were persisted.
- The run used `source_mode: api_shadow` and compare decision `api_shadow_compare_only` (compare/log only; no promotion write path).

## HAR vs runtime behavior (important differences)
- HAR contains reporting API traffic for `/reports/order-report`, `/garments/details`, `/sales-and-deliveries/sales`, and `/download-requests` with mostly `200` responses and expected `204` preflight responses.
- HAR request examples show browser-style headers (`origin`, `referer`, `content-type`) and **no explicit `Authorization` header in captured request headers**, and no `token` query parameter in those sample URLs.
- The runtime log indicates API calls were made with `auth_context_used_authorization_header: true` **and** `auth_context_used_token_query: true` (legacy auth shape), which is a notable divergence from this HAR snapshot.

## Production-readiness judgment
**Not ready for production promotion yet.**

Reasoning:
1. Shadow reconciliation is the explicit promotion gate, and this run failed that gate for all stores (`0 passed / 2 failed`).
2. Even though data compare looks clean, gate failure means operational criteria beyond row equality are not yet satisfied.
3. There is an auth-shape mismatch risk to resolve/validate against current production browser behavior (HAR suggests header/query shape may differ from the runner's legacy auth path).

## Recommended next checks before promotion
1. Open the reconciliation artifact JSON for this run and list exact per-store failure reasons.
2. Verify the expected auth strategy for production API calls (header-only vs token query vs both) and align runner configuration with the accepted contract.
3. Re-run shadow mode until reconciliation passes for all stores and confirm no gating failures remain.
4. Keep ingestion phone normalization warnings visible, but treat as data quality follow-up unless business rules require hard failure.
