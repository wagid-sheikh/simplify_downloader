## A. Context Summary (CTO View)

### 1. What exists today

At the repo root (`simplify_downloader/`):

- `dashboard_downloader/` → ETL Pipeline #1 (working)
- `crm_downloader/` → ETL Pipeline #2 (planned/empty)
- `common/` → shared DB + ingest + utility code
- `tsv_dashboard/pipelines/` → dashboard orchestration layer
- `config.py`, `crypto.py`, `__main__.py`, `simplify_downloader.py`
- `alembic/` → migrations & env
- `tests/`, `scripts/`, `Dockerfile`, `docker-compose.yml`, `docs/`

Root also has `__init__.py`, so the whole repo accidentally becomes a Python package `simplify_downloader`.

### 2. Why this is a problem

- The name **`simplify_downloader` is conceptually dead**:
  - Used in imports, CLI, Docker, Compose, tests, logs, docs.
  - Does not match what the project is now (TSV CRM backend).
- Structure is **not backend-ready**:
  - No single app package (like `app/`).
  - Code is scattered across root-level folders.
- `tsv_dashboard` feels like an extra mini-project rather than part of `dashboard_downloader`.
- Previous Codex refactors already created fragility; further changes must be controlled.

### 3. Final target architecture (non-negotiable)

On disk (local):

```text
tsv-crm/
  crm-backend/                     # this repo
  crm-frontend/                    # separate repo, colocated locally for convenience
```

Inside **`crm-backend/`**:

```text
crm-backend/
  app/                             # NEW main Python package
    __init__.py
    common/
    dashboard_downloader/
      pipelines/
    crm_downloader/
    config.py
    crypto.py
    __main__.py
    simplify_downloader.py         # legacy entrypoint, may be removed later
  alembic/
  tests/
  scripts/
  docs/
  Dockerfile
  docker-compose.yml
  pyproject.toml
```

### 4. Core principles

- Exactly **one** top-level Python package: `app/`.
- All ETL code:
  - `dashboard_downloader` → `app/dashboard_downloader/`
  - `crm_downloader` → `app/crm_downloader/`
- All dashboard orchestration:
  - `tsv_dashboard/pipelines` → `app/dashboard_downloader/pipelines/`.
- No imports from `simplify_downloader.*` remain in active code.
- All CLI entrypoints use: `python -m app ...`.

### 5. Non-negotiable constraints

- Do **not** break the working ETL pipeline.
- Do **not** modify business logic (no changes to function bodies / algorithms).
- Codex must **only** perform explicitly listed operations.
- All steps are mechanical (moves, renames, import path updates).
- Each TODO ends with verification; do not proceed until verification is green.

### 5. Naming Decision

- **Python package name:** `app`
- **pyproject project/package name:**: `tsv-crm-backend`
- **CLI style (for now):** `poetry run python -m  ...`
