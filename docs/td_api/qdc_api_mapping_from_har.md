# QuickDryCleaning – Production API Mapping (Extracted from HAR)
**Source file:** `subs.quickdrycleaning.com.har`  
**Capture date context:** Requests include `startDate=2026-02-16` / `endDate=2026-02-16`.
---
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
| Report / Feature | Method(s) | Path | Typical params / body | Response (from HAR) |
|---|---|---|---|---|
| Orders Report (list) | GET | `/reports/order-report` | `page,pageSize,startDate,endDate,expandData` | 200 / application/json / 4223 bytes |
| Sales & Delivery (sales list) | GET | `/sales-and-deliveries/sales` | `page,pageSize,startDate,endDate` | 200 / application/json / 353 bytes |
| Sales & Delivery (payment modes) | GET | `/sales-and-deliveries/payment-modes` | none | 200 / application/json / 358 bytes |
| Sales & Delivery (historical listing) | GET | `/sales-and-deliveries/historical` | `type=SALES` (observed) | 200 / application/json / 8501 bytes |
| Sales & Delivery (historical export job) | POST | `/sales-and-deliveries/historical` | `{"filters":"startDate=...&endDate=...","clientId":"1","type":"SALES"}` | 201 / application/json / 337 bytes |
| Garment-wise (details list) | GET | `/garments/details` | `page,pageSize,startDate,endDate` | 200 / application/json / 5899 bytes |
| Garment-wise (stages dimension) | GET | `/garments/stages` | none | 200 / application/json / 122 bytes |
| Garment-wise (historical listing) | GET | `/garments/historical` | none observed | 200 / application/json / 2 bytes |
| Garment-wise (historical export job) | POST | `/garments/historical` | `{"filters":"startDate=...&endDate=...","clientId":"1"}` | 201 / application/json / 281 bytes |
| Exports / downloads (poll list) | GET | `/download-requests` | none | 200 / application/json / 9115 bytes |
| Exports / downloads (create export job) | POST | `/download-requests` | `{"filters":"startDate=...&endDate=...","clientId":"1"}` | 201 / application/json / 346 bytes |

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
