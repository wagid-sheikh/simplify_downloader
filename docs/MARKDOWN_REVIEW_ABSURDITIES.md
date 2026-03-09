# Markdown Review Absurdities

## Review scope
Reviewed all `*.md` files in the repository for baseline consistency checks:

- single top-level heading (`#`)
- trailing whitespace
- obvious duplicate doc filenames

## Findings summary

### 1) Duplicate/ambiguous doc naming
- `docs/crm-sync-pipeline copy.md` appears to be a duplicate copy of `docs/crm-sync-pipeline.md` and should be archived, merged, or removed.

### 2) Files missing a top-level heading (`# ...`)
- `alembic/versions/README.md`
- `app/charms_wiki/wiki-info.md`
- `docs/StepC-Documentation.md`
- `docs/crm-sync-todo.md`
- `docs/reporting.md`
- `docs/start.md`
- `docs/tag-github.md`
- `docs/uc_page_htmls/downloading_gst_and_order_details.md`

### 3) Files with multiple top-level headings
- `docs/AGENTS.md`
- `docs/CRM-Downloader-Specs.md`
- `docs/StepA-Response.md`
- `docs/leads_assignments_pipeline.md`
- `docs/orders_sync_run_profiler_requirement.md`
- `docs/temp_md/Multi-Tenant-SaaS-Base-Input.md`
- `docs/tsv_crm_refactor_master_plan.md`

### 4) Files with trailing whitespace
- `docs/crm-sync-pipeline copy.md`
- `docs/crm-sync-pipeline.md`
- `docs/leads_assignments_pipeline.md`
- `docs/monthly_store_report_spec.md`
- `docs/tsv_crm_refactor_master_plan.md`

---

## Task: Markdown consistency cleanup and alignment

### Goal
Bring all markdown docs to a consistent baseline style to reduce confusion and noisy diffs.

### Proposed actions
1. Add exactly one `# Title` to markdown docs that currently have none.
2. Convert extra `#` headings to `##`/`###` where appropriate in docs that currently have multiple H1s.
3. Remove trailing whitespace in all markdown docs.
4. Resolve duplicate docs (start with `docs/crm-sync-pipeline copy.md`) by either:
   - merging useful content into canonical doc, then deleting duplicate, or
   - explicitly marking duplicate as archived and linking canonical file.
5. (Optional but recommended) Add markdown lint config and CI check (e.g., `markdownlint`) to keep consistency enforced.

### Acceptance criteria
- Every markdown file has exactly one H1, except intentional special cases documented in a lint ignore file.
- No trailing whitespace remains in markdown files.
- No ambiguous `* copy.md` doc remains without explicit archival rationale.
- CI or local lint command is documented for future markdown checks.
