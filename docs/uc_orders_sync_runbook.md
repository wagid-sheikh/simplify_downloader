# UC Orders Sync Runbook (API-only GST)

## Local run

Use the standard runner:

```bash
./scripts/run_local_uc_orders_sync.sh --from-date 2026-02-01 --to-date 2026-02-15
```

## Runtime controls

### HTTPS certificate handling

UC browser contexts are strict by default: `UC_IGNORE_HTTPS_ERRORS=false` in
`system_config`. This means Playwright validates the remote HTTPS certificate
chain for UC pages instead of silently accepting certificate errors.

Operational tradeoff:

- **Preferred fix:** renew, replace, or otherwise repair the remote UC HTTPS
  certificate/chain so normal TLS validation succeeds.
- **Emergency workaround only:** set `UC_IGNORE_HTTPS_ERRORS=true` only for a
  time-boxed incident when certificate repair is blocked and the business has
  accepted the risk. This suppresses browser-side HTTPS certificate validation
  for UC sync contexts and can hide misconfiguration or interception.
- After the incident, restore `UC_IGNORE_HTTPS_ERRORS=false` and re-run the UC
  sync smoke test to confirm strict validation works again.
