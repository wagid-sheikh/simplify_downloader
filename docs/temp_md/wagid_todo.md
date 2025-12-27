# Your Personal Execution Guide

**(Multi-Repo SaaS with Codex — Safe, Sequential, No Chaos)**

This guide assumes:

* SRS is frozen and approved
* AGENTS*.md files are final and approved
* Codex will write code, not decide architecture
* You are the **platform governor**, not the coder

Your job is to **set rails, release Codex inside rails, review, and merge**.

---

## PHASE 0 — MENTAL RESET (IMPORTANT)

Before touching GitHub, internalize this:

> ❝ Codex is not a developer. Codex is a very fast intern who never remembers context unless you pin it. ❞

You are not “building everything at once”.
You are **bootstrapping a governed factory**.

---

## PHASE 1 — REPOSITORY BOOTSTRAP (ONE-TIME)

### Step 1.1 — Create GitHub repositories (empty)

Create these repos (no code yet):

1. `tsv-backend`
2. `tsv-web`
3. `tsv-mobile`
4. `tsv-contracts`

**DO NOT**:

* Add frameworks
* Add README fluff
* Add CI
* Add folders

Just empty repos.

---

### Step 1.2 — Add governance files FIRST (non-negotiable)

For **each repo**, do this in order:

1. Add:

   * `AGENTS.md` (repo-specific one)
2. Add:

   * `SRS.md` (copy of the frozen SRS)
3. Add:

   * `.gitignore` (basic, minimal)
4. Commit with message:

   ```
   chore: add baseline governance and SRS
   ```

Why this matters:

* Codex reads AGENTS.md *first*
* This anchors every future action

✅ **Checkpoint:**
Each repo contains **only governance + SRS**, nothing else.

---

## PHASE 2 — CONTRACTS FIRST (DO NOT SKIP)

Everything depends on contracts. If you skip this, chaos is guaranteed.

### Step 2.1 — Contracts repo: structure only

In `tsv-contracts`, manually create:

```
/openapi/
  └── v1.yaml   (empty placeholder)
/schemas/
README.md
```

Commit:

```
chore: initialize contracts structure
```

---

### Step 2.2 — FIRST Codex task (safe + small)

Prompt Codex ONLY in `tsv-contracts`:

> “Read AGENTS.md and SRS.md.
> Create initial OpenAPI v1 with:
>
> * health endpoint
> * auth/login
> * auth/refresh
> * config snapshot delivery endpoint (as per SRS)
>   No business logic. No implementation code.”

Why:

* Tests Codex compliance
* Establishes config delivery contract early
* Very low blast radius

You review:

* Naming
* Plane prefixes
* Config endpoint existence
* No extra endpoints

Merge or request fix.

✅ **Checkpoint:**
Contracts repo has **v1 OpenAPI** with config snapshot delivery defined.

---

## PHASE 3 — BACKEND SKELETON (NO FEATURES YET)

### Step 3.1 — Backend repo: skeleton only

Prompt Codex in `tsv-backend`:

> “Read AGENTS.md and SRS.md.
> Create backend skeleton with:
>
> * FastAPI app
> * health endpoint
> * config subsystem skeleton (Redis + DB placeholders)
> * NO business features
> * NO migrations except bootstrap
> * NO workers yet”

You are checking:

* Tenant context middleware exists
* ConfigService exists
* Redis used only via config service
* No env sprawl

Merge only if clean.

---

### Step 3.2 — Backend + Contracts sync

Verify:

* Backend imports generated contract types
* Backend does NOT invent schemas

If mismatch:

* Fix contracts first
* Then backend

✅ **Checkpoint:**
Backend boots, responds to health, loads config snapshot.

---

## PHASE 4 — WEB & MOBILE SHELLS (UI WITHOUT FEATURES)

### Step 4.1 — Web repo: AppShell only

Prompt Codex in `tsv-web`:

> “Read AGENTS.md and SRS.md.
> Build Web AppShell with:
>
> * routing
> * layouts (platform, tenant, auth)
> * empty pages using templates
> * NO real features
> * consume config snapshot endpoint”

Check:

* AppShell exists
* No page-level CSS
* No flags hardcoded
* Uses contract types

---

### Step 4.2 — Mobile repo: screen templates only

Prompt Codex in `tsv-mobile`:

> “Read AGENTS.md and SRS.md.
> Build Mobile shell with:
>
> * navigation
> * offline banner
> * list/detail/form templates
> * config snapshot consumption
> * NO real features”

Check:

* Offline banner standardized
* No local flags
* Uses contracts

✅ **Checkpoint:**
Web and Mobile **render shells** but do nothing useful yet.
This is correct.

---

## PHASE 5 — FEATURE DEVELOPMENT LOOP (THE SAFE WAY)

This is where people usually panic. Don’t.

### The ONLY allowed feature workflow

For **every new feature**, always do this sequence:

#### Step A — Update SRS (small, surgical)

* Add or refine requirement
* Assign requirement ID

#### Step B — Update RTM

* Map requirement → repo(s)

#### Step C — Update contracts (if needed)

* New endpoint / field
* Version rules respected

#### Step D — Backend implementation

* One feature
* One PR
* Config via Redis snapshot only

#### Step E — Web + Mobile consumption

* Consume via contracts
* No local logic
* No flags

---

### Handling “clarity gained while coding” (THIS IS NORMAL)

When Codex discovers ambiguity:

1. Codex **must stop** (per AGENTS)
2. You do **not** hack code
3. You do this instead:

   * Update SRS (clarification)
   * Update RTM
   * Possibly update contracts
4. Resume Codex

> ❝ Clarification ≠ redesign ❞
> This is expected and healthy.

---

## PHASE 6 — HOW TO CONTROL WILD CODEX (IMPORTANT RULES)

### Rule 1 — One repo, one task

Never let Codex touch multiple repos in one prompt.

### Rule 2 — No “continue building”

Always specify:

* exact scope
* exact stop condition

Bad:

> “Build user management”

Good:

> “Add tenant_users CRUD without UI, no exports, no search”

---

### Rule 3 — You merge, not Codex

Codex writes PRs.
You:

* Read diff
* Check AGENTS rules
* Merge or reject

---

### Rule 4 — If something feels wrong, stop

The system is designed so **stopping is safe**.

Nothing breaks by pausing.
Everything breaks by rushing.

---

## PHASE 7 — WHEN YOU FEEL OVERWHELMED

Ask yourself:

* “Which phase am I in?”
* “Am I skipping contracts?”
* “Am I letting Codex decide?”

If yes → stop and reset.
