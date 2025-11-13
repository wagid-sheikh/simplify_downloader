# Dashboard Login & CSV Download Follow-Up Context

## Current Symptoms
- Playwright session is refreshed for each store, but the login flow times out while waiting for the dashboard to finish loading (`Timeout 30000ms exceeded` from `page.wait_for_load_state("networkidle")`).
- Pipeline aborts immediately after the timeout, so no CSV downloads are attempted for the configured stores (`A668`, `A817`, `A526`).
- Prior runs where the login timeout did not occur still produced empty CSV merges (`download_total = 0`), implying either blank responses or post-login redirects back to the authentication page.

## Environment & Setup Notes
- `dashboard_downloader/first_login.py` successfully creates `profiles/storage_state.json`, so persistent auth is available but may be expiring before the downloader navigates to store dashboards.
- `.env` (checked into the repo) configures `STORES_LIST="A668,A817,A526"`; use this ordering when reproducing the issue.
- Debugging with `headless=False` shows a brief "you need to login" style message before the browser closes, confirming Playwright is being redirected to the login prompt despite the stored credentials.

## Code Areas to Inspect Next Session
- `_perform_login_flow` in `dashboard_downloader/run_downloads.py`: validate that the login form submission waits for a definitive success indicator (e.g., dashboard DOM selectors) rather than generic load states.
- `_ensure_dashboard` in the same module: confirm that post-login navigation checks for authentication failures and retries with explicit error reporting.
- Download orchestration in `_download_one_spec`: ensure it revisits the dashboard between CSV pulls and handles redirects to the login screen gracefully.

## Suggested Next Steps
1. Capture HTML snapshots when login verification fails to see the exact content returned by the server.
2. Replace `wait_for_load_state("networkidle")` with targeted waits for known dashboard selectors (e.g., table headers) and add defensive timeouts with retries.
3. Introduce assertions that the current URL matches an authenticated dashboard path before starting CSV downloads; abort with actionable logs otherwise.
4. After stabilizing login, rerun `./scripts/run_dashboard_pipeline.sh` to confirm non-empty CSV merges and ingestion counts.

## Outstanding Questions for Follow-Up
- Does the dashboard require periodic OTP or CAPTCHA challenges that invalidate the saved storage state?
- Should the downloader explicitly clear cookies/session data between stores to avoid cross-store redirects?
- Are there rate limits or throttling behaviors that trigger the login prompt mid-run when multiple stores are processed back-to-back?
