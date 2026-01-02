# Multi-Tenant Foster Care Management System (SaaS) — Blueprint (UK + USA, MVP includes Carer Portal + Full Finance)

## 1. Purpose and Vision

Design and deliver a multi-tenant, SaaS-enabled Foster Care Management System that supports end-to-end fostering operations across jurisdictions (starting with UK and USA), including:

- Child and family case management
- Foster carer lifecycle management and compliance
- Placement matching, placement lifecycle, and outcomes
- Integrated finance (rates, payments, invoices, allowances, reconciliations)
- External portals (Carer Portal in MVP; partner/provider portal optional by phase)
- Reporting, statutory returns, and audit-grade compliance
- Documents, communications, and evidence management

The system must support multi-tenant onboarding, tenant-specific configuration, jurisdictional policy variants, and strict data isolation.

## 2. Scope (MVP)

### 2.1 Included in MVP

- Multi-tenant platform: tenant provisioning, tenant-scoped configuration, RBAC, audit logs
- Internal Staff App: caseworkers, supervisors, placement teams, finance users
- **Carer Portal** (MVP): secure access for carers to view placements, schedules, allowances, documents, tasks, and submit claims where allowed
- **Full Finance** (MVP): charges/rates, pay schedules, invoices, allowances, expenses, mileage rules, approvals, exports, reconciliation-ready artifacts
- Document management: templates, uploads, versioning, access control, retention metadata
- Reporting & compliance: dashboards, exports, statutory-style extracts (configurable by jurisdiction)
- Notifications: email/SMS/push (channels configurable per tenant)

### 2.2 Not in MVP (recommended phased)

- Provider/establishment portal (optional Phase 2 unless required by tenant)
- Advanced workflow orchestration engine (Phase 2+ if needed beyond MVP flows)
- AI/ML risk scoring (Phase 3+)
- Multi-region data residency (Phase 2+, after UK/USA rollout pattern validated)

## 3. Target Jurisdictions and Variation Strategy

### 3.1 UK (starting set)

- Workflows and terminology aligned with UK fostering standards (Form F, panels, reviews)
- Ofsted-driven compliance evidence patterns (audit trails, document packs)
- Statutory-style reporting extracts; tenant configurable

### 3.2 USA (starting set)

- State/agency variation is high. Support a policy configuration layer:
  - configurable approval steps
  - configurable payment schedules
  - configurable forms and document packs
- “Jurisdiction Pack” approach:
  - baseline “USA default”
  - tenant selects state pack and customizes

### 3.3 Cross-jurisdiction principles

- Normalize core domain (Child, Carer, Placement, Payments, Documents)
- Allow jurisdiction-specific overlays:
  - forms, required evidence, review cycles, payment rules, terminologies

## 4. Personas and Roles

### 4.1 Platform Roles (SaaS owner)

- Platform Admin: manage tenants, billing, global templates, platform settings, incident/audit exports

### 4.2 Tenant Roles (internal)

- Tenant Admin: configure tenant settings, users, lookups, access policies
- Caseworker/Practitioner: manage child records, assessments, plans, contacts, placement actions
- Supervisor/Manager: approvals, oversight, escalations, quality checks
- Placement Officer/Team: matching, vacancies, placement lifecycle coordination
- Finance Officer: rates, allowances, payments, invoices, reconciliations
- Panel Member/Approver: reviews/approvals for carers and placements (where applicable)

### 4.3 External Roles (MVP includes Carer Portal)

- Foster Carer (Portal): view placements, schedules, documents; submit claims/expenses; complete tasks and acknowledgements
- Carer Household Member (optional): restricted view if tenant enables

## 5. Multi-Tenant Model and Isolation

### 5.1 Tenant isolation requirement

- Every tenant’s data must be logically isolated.
- All reads/writes must be tenant-scoped and audited.

### 5.2 Recommended isolation approach (baseline)

- **Row-level tenant key** (`tenant_id`) on all tenant-scoped tables
- Enforced by application layer + DB constraints + policy tests
- Consider schema-per-tenant only if a tenant demands hard isolation/data residency later.

### 5.3 Tenant configuration

Tenant-configurable items include:

- lookup/reference data (statuses, categories, payment types, incident types)
- approval rules and role policies
- document templates/packs
- payment schedules and rates
- notification policies
- jurisdiction pack selection and overrides

## 6. Module Architecture (High-Level)

Modules are grouped by system design (mirrors the grouped wiki link inventory):

1. Platform & Access
2. Tenant & System Administration
3. External Portals (Carer Portal in MVP)
4. Provider / Partner Management (optional by tenant in MVP)
5. Child & Family Case Management
6. Foster Care — Carers (Form F + Approval)
7. Placements & Matching
8. Finance & Payments (MVP: full)
9. Reporting & Compliance
10. Documents & Communications

## 7. Module Definitions (MVP depth)

### 7.1 Platform & Access

**Scope**

- Authentication (email/password, MFA optional), session management
- Tenant selection/routing (subdomain, org code, SSO-ready)
- Baseline UI navigation patterns

**Key capabilities**

- Login/logout, password reset, account lock policies
- Session timeout, device tracking (optional)
- Access auditing

**Non-functional**

- Strong security posture, OWASP protections, audit-grade logs

---

### 7.2 Tenant & System Administration

**Scope**

- Tenant onboarding, settings, user provisioning
- RBAC and permission matrix
- Lookup/reference data management

**Key capabilities**

- Create/edit users, roles, and permissions
- Manage lookups (statuses, types, categories)
- Manage jurisdiction packs (UK/USA baseline + tenant overrides)
- Support configuration export/import (for migrations)

---

### 7.3 External Portals — Carer Portal (MVP)

**Scope**

- Secure carer access to relevant data and actions
- Carer-facing document delivery and acknowledgements

**Key capabilities**

- Carer authentication + association with household/carer record
- Placement view: active placements, schedules, key contacts
- Document center: view/download assigned documents
- Claims: submit mileage/expense/allowance claims (tenant-configurable)
- Tasks: complete acknowledgements, submit forms, upload evidence

**Constraints**

- Portal is least-privilege by default; tenants enable features per policy

---

### 7.4 Provider / Partner Management (optional by tenant in MVP)

**Scope**

- Directory of establishments/providers (care homes, supported accommodation)
- Compliance metadata and placement constraints

**Key capabilities**

- Provider profiles, contacts, service categories
- Availability/vacancy tracking (basic)
- Documents and compliance flags

---

### 7.5 Child & Family Case Management

**Scope**

- Child record system-of-record
- Family relationships, contacts, case notes
- Assessments, plans, reviews, safeguarding alerts

**Key entities**

- Child, FamilyMember/Adult, Relationship
- CaseEpisode, Assessment, Plan, Review
- Alert/Incident, ContactEvent, CaseNote

**Key capabilities**

- Create/update child record
- Capture family relationships and linked adults
- Record contact events, visits, notes
- Manage assessments, plans, reviews with due dates and reminders
- Safeguarding: incidents, flags, restricted access

---

### 7.6 Foster Care — Carers (Form F + Approval)

**Scope**

- Carer enquiry → assessment → approval → ongoing review
- Evidence and document pack management
- Panel/approval workflows

**Key entities**

- Carer, HouseholdMember, Assessment(Form F), PanelDecision, ApprovalStatus
- CarerDocuments, References, Checks (DBS/Background checks equivalents)

**Key capabilities**

- Form F workflows (versioned templates/packs)
- Capture checks, references, outcomes
- Panel scheduling/decisions
- Carer status lifecycle and compliance tracking

---

### 7.7 Placements & Matching

**Scope**

- Matching child needs to carer availability/capability
- Placement lifecycle: start, changes, respite, end, outcomes
- Contact/visits scheduling and recording

**Key entities**

- Placement, PlacementAgreement, MatchingRequest
- RespiteBooking, PlacementMove, PlacementEnd
- ContactSession, VisitLog

**Key capabilities**

- Matching requests, shortlist, placement creation
- Placement milestones and reminders
- Changes/moves and end-placement workflows
- Contact sessions logs, supervised contact

---

### 7.8 Finance & Payments (MVP full)

**Scope**

- Rates/charges engine, allowances, expenses, mileage
- Invoicing, payment approvals, exports, reconciliation support

**Key entities**

- RateCard, ChargeRule, AllowanceType
- CarerPaymentSchedule, PaymentRun, PaymentLine
- Invoice, InvoiceLine, ExpenseClaim, MileageClaim
- ApprovalWorkflow (finance)

**Key capabilities**

- Define rates, effective dates, rules (tenant-configurable)
- Compute daily/weekly/monthly charges
- Payment runs with approvals and audit
- Generate invoices (provider/carer/agency-based per tenant policy)
- Export to accounting/payroll systems (CSV/ledger format)
- Dispute handling and adjustments

**Controls**

- Full audit trail for rate changes, payment changes, approvals
- Separation of duties: configuration vs approval

---

### 7.9 Reporting & Compliance

**Scope**

- Operational dashboards, exports, statutory-style returns
- Audit-ready reporting and evidence packs

**Key capabilities**

- Standard reports by module (carers, children, placements, finance)
- Configurable extract templates by jurisdiction
- Audit logs export and retention policies
- Evidence pack generation (documents + decisions + logs)

---

### 7.10 Documents & Communications

**Scope**

- Document storage, templates, versioning, access control
- Email/SMS templates and event-driven notifications

**Key capabilities**

- Template library per tenant
- Uploads and attachments per entity
- Access roles per document; download logs
- Mail-merge style generation (where needed)
- Communication logs tied to records

## 8. Cross-Cutting Concerns

### 8.1 Security & Privacy

- RBAC + least privilege + restricted records
- Comprehensive audit logs: who/what/when/from where
- Encryption at rest, secrets handling, secure downloads

### 8.2 Workflow and Approvals

- Carer approval workflow (panel decisions)
- Finance approval workflow (payment runs, rate changes)
- Placement approvals (tenant optional)

### 8.3 Data Quality & Governance

- Duplicate detection rules
- Mandatory fields by jurisdiction
- Immutable ledger entries for finalized payments

### 8.4 Observability

- Logs, metrics, trace IDs for key workflows
- Admin console for job runs (report generation, payment runs, exports)

## 9. MVP Build Order (Suggested)

1) Platform + Tenant Admin (auth, RBAC, lookups, tenant settings)
2) Core Data Model (child, carer, placement)
3) Carer Portal (read-only → tasks/docs → claims)
4) Finance engine (rate cards → daily charge → payment runs → exports)
5) Reporting & Compliance
6) Documents & Communications (templates, logs, packs)

## 10. Traceability to Source Wiki Inventory

The grouped wiki inventory (CSV/PDF) is treated as a reference corpus.
Each item maps to:

- Module → Subgroup → Feature/Workflow
  Used by technical writing to build a full SRS with RTM.
