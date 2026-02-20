# QuickDryCleaning – Production API Mapping (Extracted from HAR)

**Source file:** `subs.quickdrycleaning.com.har`
**Capture date context:** Requests include `startDate=2026-02-16` / `endDate=2026-02-16`.
--------------------------------------------------------

## 0) High-level architecture (what your browser is doing)

- The **human-visible base app** is `https://subs.quickdrycleaning.com/` (classic ASP/ASPX pages).
- Report pages are reachable under `subs.quickdrycleaning.com/a668/App/Reports/...` and then load the newer reporting UI hosted at `https://reports.quickdrycleaning.com/`.
- The newer reporting UI calls the **report data service**: `https://reporting-api.quickdrycleaning.com`.

### Evidence in this HAR

**Legacy report pages (HTML):**

- `https://subs.quickdrycleaning.com/a668/App/Reports/Garmentdetails`
- `https://subs.quickdrycleaning.com/a668/App/Reports/Garmentdetails.aspx`
- `https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport`
- `https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport.aspx`
- `https://subs.quickdrycleaning.com/a668/App/Reports/OrderReport`
- `https://subs.quickdrycleaning.com/a668/App/Reports/OrderReport.aspx`
- `https://subs.quickdrycleaning.com/a668/App/Reports/frmItemWiseReportSummary`
- `https://subs.quickdrycleaning.com/a668/App/Reports/frmItemWiseReportSummary.aspx`

**New reporting UI (HTML):**

- `https://reports.quickdrycleaning.com/en/order-report?token=<JWT>&source=store&storeCode=a668`
- `https://reports.quickdrycleaning.com/en/sales-and-delivery?token=<JWT>&source=store&storeCode=a668`
- `https://reports.quickdrycleaning.com/en/garments?token=<JWT>&source=store&storeCode=a668`

> Note: The HAR contains the JWT token in the `reports.quickdrycleaning.com` URL. Redact it before sharing.

---

## 1) Auth / session notes

- **No `Authorization` header** and **no `Cookie` header** were present on `reporting-api.quickdrycleaning.com` calls in this capture.
- The report UI is invoked with a **JWT token in the `reports.quickdrycleaning.com` URL** (`token=<JWT>`). The API may rely on trusted origin / server-side routing rather than per-call auth headers.
- If you automate outside the browser, you may still need whatever trust mechanism exists in your environment (Cloudflare, IP allowlist, or other gateway rules).

---

## 2) Report Data APIs (reporting-api.quickdrycleaning.com)

### 2.1 Endpoint map (short & crisp)

| Report / Feature                         | Method(s) | Path                                  | Typical params / body                                                   | Response (from HAR)                 |
| ---------------------------------------- | --------- | ------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------- |
| Orders Report (list)                     | GET       | `/reports/order-report`               | `page,pageSize,startDate,endDate,expandData`                            | 200 / application/json / 4223 bytes |
| Sales & Delivery (sales list)            | GET       | `/sales-and-deliveries/sales`         | `page,pageSize,startDate,endDate`                                       | 200 / application/json / 353 bytes  |
| Sales & Delivery (payment modes)         | GET       | `/sales-and-deliveries/payment-modes` | none                                                                    | 200 / application/json / 358 bytes  |
| Sales & Delivery (historical listing)    | GET       | `/sales-and-deliveries/historical`    | `type=SALES` (observed)                                                 | 200 / application/json / 8501 bytes |
| Sales & Delivery (historical export job) | POST      | `/sales-and-deliveries/historical`    | `{"filters":"startDate=...&endDate=...","clientId":"1","type":"SALES"}` | 201 / application/json / 337 bytes  |
| Garment-wise (details list)              | GET       | `/garments/details`                   | `page,pageSize,startDate,endDate`                                       | 200 / application/json / 5899 bytes |
| Garment-wise (stages dimension)          | GET       | `/garments/stages`                    | none                                                                    | 200 / application/json / 122 bytes  |
| Garment-wise (historical listing)        | GET       | `/garments/historical`                | none observed                                                           | 200 / application/json / 2 bytes    |
| Garment-wise (historical export job)     | POST      | `/garments/historical`                | `{"filters":"startDate=...&endDate=...","clientId":"1"}`                | 201 / application/json / 281 bytes  |
| Exports / downloads (poll list)          | GET       | `/download-requests`                  | none                                                                    | 200 / application/json / 9115 bytes |
| Exports / downloads (create export job)  | POST      | `/download-requests`                  | `{"filters":"startDate=...&endDate=...","clientId":"1"}`                | 201 / application/json / 346 bytes  |

---

## 3) Standard request headers (as captured)

These headers were consistently present for `reporting-api.quickdrycleaning.com` calls:

```http
accept: */*
origin: https://reports.quickdrycleaning.com
referer: https://reports.quickdrycleaning.com/
content-type: application/json   (POST only)
user-agent: <browser UA>
```

> Browser-added `sec-fetch-*` and `sec-ch-ua-*` headers were also present; they are usually not required for server-to-server automation.

---

## 4) Copy/paste curl examples (minimal)

### 4.1 Orders Report – list

```bash
curl 'https://reporting-api.quickdrycleaning.com/reports/order-report?page=1&pageSize=10&startDate=2026-02-16&endDate=2026-02-16&expandData=false' \
  -H 'accept: */*' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/'
```

### 4.2 Sales & Delivery – sales list

```bash
curl 'https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales?page=1&pageSize=10&startDate=2026-02-16&endDate=2026-02-16' \
  -H 'accept: */*' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/'
```

### 4.3 Sales & Delivery – create historical export job

```bash
curl -X POST 'https://reporting-api.quickdrycleaning.com/sales-and-deliveries/historical' \
  -H 'content-type: application/json' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/' \
  --data-raw '{"filters":"startDate=01+Dec+2025&endDate=16+Feb+2026","clientId":"1","type":"SALES"}'
```

### 4.4 Garments – details list

```bash
curl 'https://reporting-api.quickdrycleaning.com/garments/details?page=1&pageSize=10&startDate=2026-02-16&endDate=2026-02-16' \
  -H 'accept: */*' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/'
```

### 4.5 Garments – create historical export job

```bash
curl -X POST 'https://reporting-api.quickdrycleaning.com/garments/historical' \
  -H 'content-type: application/json' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/' \
  --data-raw '{"filters":"startDate=01+Dec+2025&endDate=16+Feb+2026","clientId":"1"}'
```

### 4.6 Downloads – poll + create

```bash
curl 'https://reporting-api.quickdrycleaning.com/download-requests' \
  -H 'accept: */*' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/'

curl -X POST 'https://reporting-api.quickdrycleaning.com/download-requests' \
  -H 'content-type: application/json' \
  -H 'origin: https://reports.quickdrycleaning.com' \
  -H 'referer: https://reports.quickdrycleaning.com/' \
  --data-raw '{"filters":"startDate=01+Dec+2025&endDate=16+Feb+2026","clientId":"1"}'
```

---

## 5) Leads report (Cancelled / Pending / Completed)

- **Not present in this HAR capture**: no API paths containing `lead`, `enquiry`, `inquiry`, or `prospect` were called.
- To capture it: open the Leads report screen, switch status tabs, apply a date filter, and export once, then record a new HAR.

---

## 6) Known limitations of this specific HAR

- For `reporting-api.quickdrycleaning.com` entries, the HAR includes **request headers**, **query params**, and **POST bodies**.
- It includes **status codes and response sizes**, but **does not include the JSON response bodies** for those endpoints in this capture.
  - (The HAR *does* include JSON bodies for certain legacy `subs.quickdrycleaning.com` ASMX calls, so body capture is partially enabled.)


---





You are a senior-most backend automation developer. Deeply review the existing production code path `td_orders_sync` and the two project artifacts:

1) `docs/td_api/qdc_api_mapping_from_har.md` (work-in-progress mapping notes)
2) `docs/td_api/subs.quickdrycleaning.com.har` (HAR contains actual API calls + responses with full fields)

Context (simple words):

- Current `td_orders_sync` heavily relies on manual UI-driven extraction (Playwright scraping).
- The same information is available through the QDC / TumbleDry backend APIs (as seen in the HAR).
- Besides Orders and Sales & Delivery, I also want to pull “Garment-wise” data (to act as order details / line items).
- This code is already in production, so the migration must be safe, staged, and reversible.

Your tasks:
A) Understand current behavior

1. Read and summarize what `td_orders_sync` does today:
   - What entities it extracts (orders, sales, delivery, customer, payments, etc.)
   - What fields it produces and where it stores them (DB tables / files / API to my system)
   - What date ranges / incremental sync logic exists
   - How it handles retries, rate limits, partial failures, and idempotency
   - What “truth source” assumptions exist today (UI pages, HTML selectors, etc.)

B) Extract the API contract from HAR
2. From the HAR file, identify:

- Base domains, endpoints, HTTP methods, required headers, auth mechanism (cookies/JWT/bearer)
- Request params (pagination, date filters, store codes, status filters)
- Response schemas (field names, nested objects, IDs)
- Any “supporting calls” needed (lookup endpoints, master data endpoints)
- Rate-limiting or anti-bot signals (if visible)

3. Use `qdc_api_mapping_from_har.md` as a starting mapping, but treat HAR as the source of truth.
   - Correct any wrong mappings.
   - Add missing endpoint mappings.
   - Create a clean field-level mapping table: UI-derived field -> API field -> transformation -> destination field/table.

C) Design the migration path (UI -> API)
4. Propose a step-by-step migration plan that is safe for production:

- Stage 0: Instrumentation & baseline (compare UI vs API outputs)
- Stage 1: Dual-run / shadow mode (API fetch + UI scrape, compare, do not write)
- Stage 2: API becomes primary, UI as fallback
- Stage 3: Remove UI dependency (keep emergency toggle)
- Include feature flags, config switches, and rollback strategy.

5. Define architecture changes:
   - A dedicated `td_api_client` module with:
     * auth/session handling
     * request signing/headers/cookies
     * pagination helpers
     * retry/backoff
     * rate limit handling
     * structured logging + correlation IDs
   - A data normalization layer that converts API response -> internal canonical schema used by `td_orders_sync`.
   - A persistence layer that remains unchanged as much as possible (so the rest of pipeline isn’t rewritten).

D) Garment-wise / order line item extraction
6. Identify how “garment-wise” data can be pulled:

- Determine which endpoint(s) provide garment/item line items, quantities, pricing, discounts, taxes, service type, status per item.
- If it requires calling “order details” per order, design an efficient strategy (batching, concurrency limits, caching).
- Define the new internal schema/table you recommend for garment-wise records (primary keys, foreign keys, unique constraints, indexes).
- Explain how to keep it incremental and idempotent.

E) Edge cases & operational realities
7. List risks and mitigations:

- Missing fields / inconsistent data between UI and API
- Timezone/date boundaries
- Partial refunds / cancellations / edits after order close
- Delivery status changes after initial sync
- Backfills and re-sync strategy
- Security concerns (storing cookies/tokens, secret rotation)

8. Deliver concrete outputs:
   - A “Migration Plan” document (steps + checklists)
   - Endpoint inventory (table)
   - Field mapping table (table)
   - Proposed module/file structure changes
   - Pseudocode for the new API-based sync flow (including incremental sync and garment-wise sync)
   - Minimal diff approach: identify what can stay unchanged and what must change.

Important constraints:

- Do NOT hand-wave. Use actual endpoints/fields from HAR.
- Be explicit about what you can infer vs what must be verified by running.
- Assume production reliability matters more than speed.
- Keep the plan implementable by one developer.

Questions you must ask me (only after your analysis):

- List any missing details you need to finalize the plan (e.g., where the data is persisted, store identifiers, auth lifecycle, expected sync frequency, acceptable lag).
- Ask specifically whether I can provide sample DB schema or example output payloads currently produced by `td_orders_sync`.

Now proceed:

1) First, summarize current `td_orders_sync`.
2) Then summarize the API surface from HAR.
3) Then give the staged migration plan and the garment-wise design.
