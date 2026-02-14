# Run Log Deep Review (scripts/run_log.txt)

- Total entries: **3760**
- Status mix: **ok=3612**, **info=62**, **warn=33**, **debug=27**, **null_status=26**
- Window results: **17** total (success=15, partial=1, success_with_warnings=1)
- Run summary updates: **17** total (success=15, partial=1, success_with_warnings=1)

## Successes
- No explicit `error`/`failed` statuses were emitted in the log.
- High volume of successful archive retrieval: "Invoice API request succeeded" appears **1319** times (first at line 155).
- Archive page retrieval also stable: "Archive API page fetched" appears **48** times (first at line 153).

## Failures / Partial outcomes
- There are **no hard failures**, but there are degradations:
  - `partial` outcome: **1** window and **1** run-summary update.
  - `success_with_warnings` outcome: **1** window and **1** run-summary update.
- Degradation-related warning messages include:
  - "Archive API total mismatch detected" (count 1, first at line 2109).
  - "UC archive order-details publish failed; continuing to payments publish" (count 1, first at line 2516).

## Warnings
- Warning frequency by message:
  - 8× Temporarily restricting TD orders discovery to a subset of stores (first at line 12)
  - 8× Probed session with existing storage state (first at line 59)
  - 4× Date range text did not update to expected value (first at line 3472)
  - 4× Failed to set date range via date-range control (first at line 3473)
  - 4× TD Orders workbook ingested with warnings (first at line 3554)
  - 3× TD Sales workbook ingested with warnings (first at line 3625)
  - 1× Archive API total mismatch detected (first at line 2109)
  - 1× UC archive order-details publish failed; continuing to payments publish (first at line 2516)

## Noise (high-volume low-signal logging)
- "Resolved archive bearer token diagnostics": **1367** entries (first at line 141).
- "Invoice API request succeeded": **1319** entries (first at line 155).
- "Orders left-nav snapshot skipped because DOM logging is disabled": **16** entries.
- "Iframe hydration observations skipped because DOM logging is disabled": **16** entries.
- 26 entries have `status: null`; these reduce status-based filtering quality (examples listed below).
  - Example 1: line 117 message="Apply inputs populated after date selection"
  - Example 2: line 126 message="GST report download saved"
  - Example 3: line 130 message="Apply inputs populated after date selection"
  - Example 4: line 145 message="GST report download saved"
  - Example 5: line 1001 message="Sales navigation outcome"
  - Example 6: line 1025 message="Sales navigation outcome"

## Improvement areas
1. Normalize status values: prevent `null` statuses; enforce enum at logger entrypoint.
2. Add severity model separate from execution status to classify diagnostics vs actionable warnings.
3. Collapse repetitive archive diagnostics into periodic counters (e.g., every 50 records) to reduce log volume.
4. Promote partial/success_with_warnings causes into structured fields (`warning_codes`, `degradation_reason`) for easier alerting.
5. For date-range warnings, include automatic retry result and fallback path outcome in one final consolidated event.
6. For TD ingest warnings, include a stable warning code (e.g., `PHONE_FALLBACK_APPLIED`) to support trend reporting.
