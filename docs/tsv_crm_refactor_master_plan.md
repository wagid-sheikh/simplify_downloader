# TSV CRM Backend – Master Refactor Plan (v1.18 → app-based architecture)

# FULLY AUDITED • CTO-LEVEL VERSION

This document is the **single source of truth** for restructuring the legacy `simplify_downloader` repo into a clean, modern, FastAPI-ready **TSV CRM Backend**.

Every TODO includes:

- **Actor:** Human (Wagid) or Codex
- **Strict Codex Prompt:** Zero-hallucination instructions Codex must follow
- **Verification:** Checklist + concrete commands

Use this file over multiple days. Always complete one TODO fully (including verification) before moving to the next.

---

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

---

## B. PERFECT TODO LIST (CTO-Audited)

Execute these in order. Never skip ahead.

---

### ✅ TODO 0 – Stable Snapshot Tag (already done)

**Actor:** Human (Wagid) – *Completed earlier*

You already did:

```bash
git tag -a v1.18 -m "Stable after round 1 refactor & Editable Daily Store Performance Report PDF"
git push origin v1.18
```

**Verification (for record only):**

- [X] `git tag` shows `v1.18`
- [X] `git show v1.1`8 shows expected commit

---

### 🥇 TODO 1 – Create Stable Parallel Instance of `v1.18` (RUN-ONLY COPY)

**Actor:** Human (Wagid)

#### Goal

Have a **frozen copy** of `v1.18` used **only to run pipelines**, while the original repo becomes the refactor playground.

#### Steps (Human)

From your main projects directory:

```bash
cd /path/where/you/keep/projects
git clone https://github.com/wagid-sheikh/simplify_downloader.git simplify_downloader_v1_18_stable
cd simplify_downloader_v1_18_stable
git checkout v1.18
git checkout -b v1.18-stable
mkdir -p reports
mkdir -p logs
poetry install
poetry run pytest
./scripts/run_dashboard_pipeline_single_context.sh
```

**Rule:**

- `simplify_downloader_v1_18_stable/` is **RUN-ONLY**:
  - No edits, no Codex, no refactors.

#### Verification

- [X] Folder `simplify_downloader_v1_18_stable/` exists.
- [X] `git status` is clean; branch is `v1.18-stable`.
- [X] `git rev-parse HEAD` equals `git rev-parse v1.18`.
- [X] `poetry run pytest` passes.
- [X] `./scripts/run_dashboard_pipeline_single_context.sh` runs successfully.

---

### 🥈 TODO 2 – Document Target Architecture & Rules (`docs/REFACTOR_PLAN.md`)

**Actor:** Human (Wagid)

#### Goal

Have a **single internal doc** that Codex and you can both refer to during refactors.

#### Steps

From refactor repo (original folder):

```bash
cd /path/to/simplify_downloader
mkdir -p docs
touch docs/REFACTOR_PLAN.md
```

Open `docs/REFACTOR_PLAN.md` and paste:

- The context summary (Section A of this file, shortened if you like).
- Final target architecture.
- Non-negotiable constraints (no logic changes, etc.).
- Statement:
  > We will kill all `simplify_downloader` references in imports, entrypoints, Docker, Compose, tests, and configs. Only archival docs may retain the old name explicitly.
  >

Commit:

```bash
git add docs/REFACTOR_PLAN.md
git commit -m "Add REFACTOR_PLAN with target architecture and constraints"
```

#### Verification

- [X] `docs/REFACTOR_PLAN.md` exists.
- [X] It contains target structure + constraints + “kill simplify_downloader” rule.
- [X] `git status` clean.

---

### 🥉 TODO 3 – Finalize Naming (Package, Project, CLI)

**Actor:** Human (Wagid)

#### Goal

Freeze key names so Codex doesn’t reintroduce churn.

#### Naming decisions

- **Python package name:** `app`
- **pyproject project name:** pick one, e.g.:
  - `tsv-crm-backend`
- **CLI style (for now):**
  - `poetry run python -m  ...`

#### Steps

1. Edit `docs/REFACTOR_PLAN.md` and add:

```markdown
## Naming Decisions

- Python package name: `app`
- Project/package name (pyproject): `<your choice here>`
- CLI: `python -m app ...` (replacing `python -m simplify_downloader ...`)
```

2. Commit:

```bash
git add docs/REFACTOR_PLAN.md
git commit -m "Document naming decisions for app package and CLI"
```

#### Verification

- [X] Naming decisions clearly documented.
- [X] `git status` clean.

---

### 🧩 TODO 4 – Create `app/` Package & Move All Python Code (STRUCTURE ONLY)

**Actor:** Codex
**Human:** Prepare branch + verify after

---

#### Human Preparation (before Codex)

From refactor repo:

```bash
cd /path/to/simplify_downloader
git checkout -b refactor/app-package-setup
git status  # must be clean
```

---

#### CODEX PROMPT (STRICT MODE – TODO 4)

Paste this EXACTLY into Codex:

```text
### CODEX STRICT MODE — TODO 4: CREATE `app/` PACKAGE AND MOVE ALL PYTHON CODE

You are performing a MECHANICAL REFACTOR.  
NO business logic changes.

DO NOT:
- Modify function bodies or algorithms
- Add or remove features
- Change CLI behavior or flags
- Touch Dockerfile, docker-compose.yml, scripts/, or pyproject.toml
- Move or rename anything beyond what is listed below

DO ONLY THE FOLLOWING:

1. Create a top-level Python package:
   - Create folder `app/` at repo root.
   - Create file `app/__init__.py`.

2. Move these DIRECTORIES into `app/` (preserve internal structure):
   - `common/` → `app/common/`
   - `dashboard_downloader/` → `app/dashboard_downloader/`
   - `crm_downloader/` → `app/crm_downloader/`
   - `tsv_dashboard/` → `app/tsv_dashboard/`

3. Move these FILES into `app/`:
   - `__main__.py` → `app/__main__.py`
   - `simplify_downloader.py` → `app/simplify_downloader.py`
   - `config.py` → `app/config.py`
   - `crypto.py` → `app/crypto.py`

4. Remove root-level `__init__.py` so the repo root is no longer a package.

5. Update ALL Python imports that currently reference `simplify_downloader.` so they reference `app.` instead. For example:
   - `from simplify_downloader.common.db import session_scope`
     → `from app.common.db import session_scope`
   - `from simplify_downloader.config import config`
     → `from app.config import config`
   - `from simplify_downloader.common.ingest.models import Base`
     → `from app.common.ingest.models import Base`

   Apply this systematically in:
   - `app/common/**`
   - `app/dashboard_downloader/**`
   - `app/crm_downloader/**`
   - `app/tsv_dashboard/**`
   - `alembic/env.py`
   - `alembic/versions/*.py` (only if they import from simplify_downloader)
   - `tests/**`

6. DO NOT:
   - Change any function or class bodies
   - Change log filenames
   - Change configuration semantics

7. When finished, output:
   - A list of files moved
   - A list of files where imports were updated
   - Any files you intentionally skipped (with reason)

STOP AFTER COMPLETING STEP 5–7.  
DO NOT perform any additional refactor steps.

### END STRICT MODE
```

---

#### Verification (Human)

From repo root:

```bash
poetry install
poetry run pytest
poetry run alembic upgrade head
./scripts/run_dashboard_pipeline_single_context.sh
```

Checklist:

- [ ] `app/` exists and has `__init__.py`.
- [ ] `common/`, `dashboard_downloader/`, `crm_downloader/`, `tsv_dashboard/` now live under `app/` only.
- [ ] No `__init__.py` at repo root.
- [ ] `grep -R "from simplify_downloader" .` and `grep -R "import simplify_downloader" .` return **no active imports**.
- [ ] Tests pass.
- [ ] Alembic upgrade works.
- [ ] Pipeline script runs end-to-end.

If something fails, fix/import issues before moving to TODO 5.

---

### 🧱 TODO 5 – Merge `tsv_dashboard/pipelines` into `app/dashboard_downloader/pipelines`

**Actor:** Codex
**Human:** Verify and run pipeline

---

#### CODEX PROMPT (STRICT MODE – TODO 5)

```text
### CODEX STRICT MODE — TODO 5: MERGE `tsv_dashboard/pipelines` INTO `app/dashboard_downloader/pipelines`

You are continuing the MECHANICAL REFACTOR after TODO 4.

CURRENT STRUCTURE (assumed):
- `app/dashboard_downloader/` exists.
- `app/tsv_dashboard/pipelines/` contains:
  - base.py
  - dashboard_monthly.py
  - dashboard_weekly.py
  - reporting.py

YOUR TASK (ONLY THESE OPERATIONS):

1. Ensure directory:
   - `app/dashboard_downloader/pipelines/` exists.
   - If missing, create it and add `app/dashboard_downloader/pipelines/__init__.py`.

2. Move the following files:
   - `app/tsv_dashboard/pipelines/base.py`
     → `app/dashboard_downloader/pipelines/base.py`
   - `app/tsv_dashboard/pipelines/dashboard_monthly.py`
     → `app/dashboard_downloader/pipelines/dashboard_monthly.py`
   - `app/tsv_dashboard/pipelines/dashboard_weekly.py`
     → `app/dashboard_downloader/pipelines/dashboard_weekly.py`
   - `app/tsv_dashboard/pipelines/reporting.py`
     → `app/dashboard_downloader/pipelines/reporting.py`

3. Update imports INSIDE these moved files:
   - Replace any occurrences of `simplify_downloader.` with `app.` (if any remain).
   - Replace imports from `tsv_dashboard` with imports from `app.dashboard_downloader.pipelines` or other appropriate `app.*` modules.

4. Update all OTHER modules that import from:
   - `app.tsv_dashboard.pipelines.*`
   so they instead import from:
   - `app.dashboard_downloader.pipelines.*`

5. If, after this, `app/tsv_dashboard/` is empty or unused, remove that directory safely.

CONSTRAINTS:
- Do NOT modify function or class bodies beyond necessary import path changes.
- Do NOT touch Dockerfile, docker-compose.yml, scripts/, or pyproject.toml here.
- STOP after the above steps.

When finished, output:
- Files moved
- Files updated (imports changed)
- Whether `app/tsv_dashboard/` was removed

### END STRICT MODE
```

---

#### Verification

Commands:

```bash
poetry run pytest
./scripts/run_dashboard_pipeline_single_context.sh
```

Checklist:

- [ ] `app/dashboard_downloader/pipelines/` contains `base.py`, `dashboard_monthly.py`, `dashboard_weekly.py`, `reporting.py`.
- [ ] No imports reference `tsv_dashboard` in code.
- [ ] `app/tsv_dashboard/` was removed or confirmed unused.
- [ ] Tests pass.
- [ ] Pipeline outputs are as expected.

---

### 🧹 TODO 6 – Remove `simplify_downloader` Identity from Active Use (Code, Tooling, Docs, CI)

**Actor:** Codex
**Human:** Carefully review

#### Goal

Remove **all active uses** of `simplify_downloader` in:

- Code imports / fake modules
- CLI/entrypoints (Docker, Compose, scripts, CI)
- Config/log paths
- Runtime docs (README, docs, reports)
- Package metadata

Allow `simplify_downloader` only in **clearly historical** mentions (e.g. “previous project name” section).

---

#### CODEX PROMPT (STRICT MODE – TODO 6)

```text
### CODEX STRICT MODE — TODO 6: REMOVE ACTIVE `simplify_downloader` REFERENCES (FULL SWEEP)

You are continuing the MECHANICAL REFACTOR. Package `app` is now the main entrypoint.

Use this as your authoritative list of what to fix. The following categories are known from a prior "laundry list" grep.

YOU MUST UPDATE:

1. Entry points & tooling
   - Dockerfile:
       ENTRYPOINT ["python", "-m", "simplify_downloader"]
       → ["python", "-m", "app"]
   - docker-compose.yml:
       command: ["python", "-m", "simplify_downloader", ...]
       → ["python", "-m", "app", ...]
   - scripts/run_dashboard_pipeline_single_context.sh (and similar scripts):
       python -m simplify_downloader ...
       → python -m app ...
   - .github/workflows/deploy-prod.yml:
       python -m simplify_downloader db upgrade
       → python -m app db upgrade

2. Runtime Python imports / fake modules
   - Replace ALL imports from `simplify_downloader.*` with `app.*` in:
       - app/dashboard_downloader/**
       - app/common/**
       - app/tsv_dashboard/** (if still present)
       - app/config.py, app/crypto.py
       - alembic/env.py
       - alembic/versions/*.py where they import from simplify_downloader
       - tests/**
   - In tests that fake modules:
       sys.modules["simplify_downloader.config"]
       → sys.modules["app.config"]
     and any types.ModuleType("simplify_downloader.config") → "app.config".

3. Config & log paths
   - .env, .env.example:
       JSON_LOG_FILE="logs/simplify_downloader.jsonl"
       → JSON_LOG_FILE="logs/app.jsonl"  (or another agreed neutral name)
   - tests/conftest.py:
       tests/logs/simplify_downloader.jsonl
       → tests/logs/app.jsonl
   - Any other hardcoded "logs/simplify_downloader.jsonl" → consistent new name.

4. Package metadata & CLI display name
   - pyproject.toml:
       name = "simplify-downloader"
       → name = "<FINAL PROJECT NAME>" (e.g. "tsv-crm-backend")
   - dashboard_downloader/cli.py:
       argparse.ArgumentParser(prog="simplify_downloader")
       → argparse.ArgumentParser(prog="<new cli name>")  # e.g. "tsvcrm" or "app"
     Use the naming decision documented in docs/REFACTOR_PLAN.md.

5. Runtime documentation (not just history)
   Update any run instructions or descriptions that describe how to operate the system:
   - README.md:
       poetry run python -m simplify_downloader ...
       → poetry run python -m app ...
   - docs/CODEX_KICKOFF_SIMPLIFY_DOWNLOADER.md:
       "run `python -m simplify_downloader` inside container"
       → use `python -m app`
   - docs/reporting.md:
       "You are working in the `simplify_downloader` project."
       → reflect the new project name (TSV CRM backend / tsv-crm-backend).
   - reports/2025/*.md:
       Any instructions that execute the `simplify_downloader` CLI
       → must show `python -m app` instead.

6. Historical / archival references
   - You MAY keep a small historical note like:
       "This project was previously named `simplify_downloader`."
     but only in one or two central places (e.g. README History section).
   - DO NOT leave scattered old name usage in operational docs.

7. What NOT to touch
   - Do NOT change function or class logic.
   - Do NOT rename the `app` package.
   - Do NOT modify browser profile paths, __pycache__, or .git internals.
   - These are considered ephemeral or tooling internal.

When done, OUTPUT:
- A grouped list of files changed under:
  - entrypoints (Docker/compose/scripts/workflows)
  - python imports
  - env/log paths
  - docs/reports
  - metadata/CLI

STOP after completing the steps above.

### END STRICT MODE
```

---

#### Verification (Human)

1. Search:

```bash
grep -R "simplify_downloader" .
```

Confirm:

- [ ] No matches in:
  - `app/` (code)
  - `alembic/`
  - `tests/`
  - `Dockerfile`
  - `docker-compose.yml`
  - `scripts/`
  - `.env`, `.env.example`
  - `.github/workflows/`
  - `README.md`
  - `docs/`
  - `reports/2025/`
- [ ] Any remaining matches are **explicitly historical** (e.g., “previously called simplify_downloader”) in a small, controlled number of places.

2. Run:

```bash
poetry run pytest
poetry run alembic upgrade head
./scripts/run_dashboard_pipeline_single_context.sh
```

- [ ] All commands succeed.
- [ ] Pipelines now run via `python -m app` entrypoints end-to-end.

---

### ✅ TODO 6A – Laundry-List Burn-Down (Final Sanity)

**Actor:** Human (Wagid)

Use this to tick off **every category** from the original `laundry_list.txt` sweep.

- [ ] **Code imports**

  - No `from simplify_downloader...` or `import simplify_downloader` in any `.py` file.
- [ ] **Alembic**

  - `alembic/env.py` and all `alembic/versions/*.py` updated to `app.*` imports only.
- [ ] **Tests**

  - No `simplify_downloader.config` in `tests/`.
  - All fake-module injections and log paths updated.
- [ ] **Docker & Compose**

  - Dockerfile `ENTRYPOINT` uses `python -m app`.
  - `docker-compose.yml` uses `python -m app`.
- [ ] **Scripts**

  - `scripts/run_dashboard_pipeline_single_context.sh` (and siblings) call `python -m app`.
- [ ] **CI (GitHub workflows)**

  - `.github/workflows/deploy-prod.yml` runs `python -m app db upgrade`.
- [ ] **Config & Logs**

  - `.env` / `.env.example` JSON_LOG_FILE path no longer references `simplify_downloader`.
  - Tests now use a neutral path, e.g. `tests/logs/app.jsonl`.
- [ ] **Docs & Reports**

  - README, `docs/*.md`, `reports/2025/*.md` do **not** instruct to run `simplify_downloader` anymore.
  - If the old name is mentioned, it is clearly labelled as “former/previous name” only.
- [ ] **Metadata & CLI**

  - `pyproject.toml` no longer uses `simplify-downloader` as `name`.
  - Any `prog="simplify_downloader"` updated to the new CLI name.
- [ ] **Ephemeral / Internal (Conscious decision to ignore)**

  - You have decided to ignore:
    - `__pycache__` entries
    - Browser profile LevelDB paths under `profiles/`
    - `.git/config` remote name
  - These do not affect production readiness.

---

### 🧪 TODO 7 – Full Regression & Output Comparison

**Actor:** Human (Wagid)

#### Goal

Confirm that the refactored repo behaves the same as v1.18 stable for at least one known period.

#### Steps

1. Use the **v1.18 stable** repo to run the pipeline for a specific known date or period.
2. Use the **refactored repo** to run the pipeline for the same period.
3. Compare:
   - Generated PDFs
   - Key DB tables (spot-check, or export to CSV and compare)
   - Any logs you care about

You don’t need bitwise identity, but operationally they should represent the same business data.

#### Verification

- [ ] v1.18 stable pipeline output looks correct.
- [ ] Refactored pipeline output matches expectations and is consistent.
- [ ] No new regressions observed.

---

### 🚀 TODO 8 – FastAPI Phase (OUT OF SCOPE FOR THIS FILE)

**Actor:** Human + Codex (future)

Only after all previous TODOs are solid:

- Introduce `app/api/`, `app/core/`, `app/models/`, etc.
- Expose ETL control + reporting endpoints by calling into:
  - `app.dashboard_downloader.*`
  - `app.crm_downloader.*`
- Implement auth, API config, and other backend concerns.

This is a separate project phase and not covered here.

---

**End of TSV CRM Backend – Master Refactor Plan (CTO-Audited).**
