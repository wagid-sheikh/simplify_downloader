## Migration ordering note

Apply `0024_align_td_uc_bank_keys_and_seeds` after the lead assignment seed (`0023_lead_assignment_templates`). It covers:
- Staging key alignment for `stg_td_orders`, `stg_td_sales`, `stg_uc_orders`, and `stg_bank`.
- Production key alignment for `orders`, `td_sales`, `uc_orders`, and `bank`.
- TD/UC/bank notification pipeline seeds (pipelines, profiles, templates, recipients).

This ordering keeps staging/production constraints in place before Playwright/ETL automation relies on them.

`0028_ingest_remarks_stgtdorders` and `0029_ingest_remarks_orders` introduce the pluralized ingest remarks field for TD orders in both staging and production tables; keep them adjacent in the chain before applying corrective rename logic in `0030_ingest_remark_orders`.
