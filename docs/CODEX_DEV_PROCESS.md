# Codex Development Control Process (Master Wrapper)

This markdown file defines a strict, auditable workflow for Codex when developing or modifying the **TSV CRM Backend** project (or any future project). Store it in the repo (e.g., `/docs/CODEX_DEV_PROCESS.md`) and reference it in each development prompt.

---

## 🧭 Overview

This file ensures Codex behaves like a disciplined senior engineer:

1. Always pauses to ask questions before coding.
2. Executes work in clear, gated phases.
3. Performs a self-audit and verification rubric before final delivery.
4. Never makes silent assumptions.

---

## 🔒 Assumptions Policy

* Codex **must not assume** any behavior or schema without confirmation.
* When unclear, Codex must list each assumption explicitly and mark it as *pending confirmation*.
* Each assumption must include:

  * Rationale
  * Risk if wrong
  * Proposed test or validation method
* Any hidden assumption that goes unlisted = automatic **self-audit failure**.

---

## ⚙️ Phase Workflow

### **PHASE 1 — Clarifying Questions Only**

Purpose: understand scope before touching code.

Output format:

| #   | Topic          | Question                                           | Why it matters        | Current Assumption | Risk if Wrong  |
| --- | -------------- | -------------------------------------------------- | --------------------- | ------------------ | -------------- |
| 1   | e.g. DB design | Should each bucket table have its own primary key? | Ensures upsert works. | Yes, composite PK  | Duplicate rows |

Then:

* Summarize a **5–10 bullet plan** for implementation.
* End the message with:

  ```
  === AWAITING APPROVAL — DO NOT PROCEED ===
  ```

### **PHASE 2 — Implementation Plan**

* Expand plan → per-file change list.
* Identify dependencies (DB, Alembic, Docker, CI/CD).
* End with the same gate marker.

### **PHASE 3 — Code Changes**

* Implement per file, showing:

  ```
  [FILE] path/to/file.py
  [WHY] reason for change
  [DIFF] unified diff here
  ```
* After all diffs:

  * Provide **BUILD COMMANDS** (run, lint, test, migrate).
  * End with `=== AWAITING APPROVAL — DO NOT PROCEED ===`

### **PHASE 4 — Self-Audit & Tests**

* Run the full self-audit checklist (see below).
* Provide evidence with file:line references.
* End with same gate.

### **PHASE 5 — Final Delivery Package**

* Summarize all decisions, confirmed assumptions, risks, and remaining TODOs.
* Include a reproducible **Run / Migrate / Verify** section.

---

## ✅ Self-Audit Checklist

Codex must produce this table after implementation.

| Checklist Item                 | Result | Evidence (file:line)              | Follow-up |
| ------------------------------ | ------ | --------------------------------- | --------- |
| Requirements coverage          | Pass   | ingestion.py:120                  | —         |
| Fault tolerance                | Pass   | merge.py:88                       | —         |
| Secrets not exposed            | Pass   | docker-compose.yml:45             | —         |
| Structured JSON logging        | Pass   | json_logger.py:30                 | —         |
| Audit counts logged            | Pass   | audit.py:110                      | —         |
| Cleanup safe & conditional     | Pass   | cleanup.py:64                     | —         |
| Alembic migrations generated   | Pass   | versions/xxxx_init.py:1           | —         |
| Docker runs app + db correctly | Pass   | compose.yml:20                    | —         |
| CI/CD deploy tested            | Pass   | .github/workflows/deploy-prod.yml | —         |
| Tests added & passing          | Pass   | tests/test_ingestion.py:40        | —         |

Any item failing must be listed under **KNOWN GAPS / TODOs** with owner & ETA.

---

## 🧪 Final Verification Rubric

Codex must run this rubric **after** self-audit.

| Criterion                | Description                                  | Pass/Fail |
| ------------------------ | -------------------------------------------- | --------- |
| Gates followed           | Each phase ended with explicit approval gate |           |
| Diff traceability        | Every changed file has [WHY] + [DIFF]        |           |
| Logs & audit implemented | JSON logs show counts + cleanup              |           |
| Alembic + ORM consistent | Models match DB migrations                   |           |
| Dockerized runtime       | App + DB run via docker-compose              |           |
| SSH tunnel only exposure | No public DB ports                           |           |
| CI/CD reproducible       | Workflows run end-to-end                     |           |
| Idempotency verified     | Re-run yields identical DB state             |           |
| Documentation updated    | README + this file referenced                |           |

Codex must append the rubric with its results and evidence.

---

## 🧰 Audit & Cleanup Policy

For each bucket/date:

1. Log row counts `{download_total_by_store, merged_rows, ingested_rows}`.
2. Compare:

   * if all counts match → delete individual + merged CSVs.
   * else → keep files, log discrepancy.
3. All logs go through **json_logger** with fields:

   ```json
   {
     "ts": "2025-11-12T14:00:00Z",
     "run_id": "20251112_A668",
     "phase": "audit",
     "bucket": "missed_leads",
     "counts": {"download": 123, "merged": 123, "ingested": 123},
     "status": "ok",
     "message": "counts match, files deleted"
   }
   ```

---

## 🔐 Command Markers

Use these exact markers for control:

* End of each phase: `=== AWAITING APPROVAL — DO NOT PROCEED ===`
* Begin next phase: `PHASE X — <name>`

Codex must **not continue** until user approval is given.

---

## 🧩 Re-run & Double-Check Commands

To re-trigger verification:

```
PHASE SELF-AUDIT
Run SELF-AUDIT CHECKLIST and FINAL VERIFICATION RUBRIC on current codebase.
List all Pass/Fail items with evidence.
```

To force Codex to verify all work before finishing:

```
FINAL REVIEW REQUIRED
Run complete self-audit + rubric + list of remaining TODOs.
Return only that output.
```

---

## 💡 Usage

In any future prompt, simply include this line:

```
Follow /docs/CODEX_DEV_PROCESS.md exactly.
```

Codex will load and obey this process automatically.
