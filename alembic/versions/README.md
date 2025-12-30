## Migration ordering note

Apply `0024_align_td_uc_bank_keys_and_seeds` after the lead assignment seed (`0023_lead_assignment_templates`). It covers:
- Staging key alignment for `stg_td_orders`, `stg_td_sales`, `stg_uc_orders`, and `stg_bank`.
- Production key alignment for `orders`, `td_sales`, `uc_orders`, and `bank`.
- TD/UC/bank notification pipeline seeds (pipelines, profiles, templates, recipients).

This ordering keeps staging/production constraints in place before Playwright/ETL automation relies on them.
