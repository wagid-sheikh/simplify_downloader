# Leads Assignment Validation Checklist

Use the following SQL probes to validate the end-to-end leads assignment pipeline. Each section lists the intent of the check and the query to run.

## 1) Store eligibility (opt-in flag on `store_master`)
Verify which stores are marked as lead-assignment eligible and whether they already have mappings.

```sql
-- Eligible stores and whether they have at least one mapping
SELECT
    sm.store_code,
    sm.store_name,
    sm.assign_leads,
    COUNT(slam.id) FILTER (WHERE slam.is_enabled) AS active_mappings,
    COUNT(slam.id) AS total_mappings
FROM store_master sm
LEFT JOIN store_lead_assignment_map slam ON slam.store_code = sm.store_code
WHERE sm.assign_leads = true
GROUP BY sm.store_code, sm.store_name, sm.assign_leads
ORDER BY sm.store_code;
```

## 2) Store ↔ agent mappings (caps and enablement)
Confirm the mapping rows that drive assignment and their caps.

```sql
SELECT
    slam.store_code,
    am.agent_code,
    am.agent_name,
    slam.is_enabled,
    slam.priority,
    slam.max_existing_per_lot,
    slam.max_new_per_lot,
    slam.max_daily_leads
FROM store_lead_assignment_map slam
JOIN agents_master am ON am.id = slam.agent_id
ORDER BY slam.store_code, slam.is_enabled DESC, slam.priority, am.agent_code;
```

## 3) Agent activity (lifecycle + recent usage)
Spot inactive agents or agents that have not received assignments recently.

```sql
SELECT
    am.id,
    am.agent_code,
    am.agent_name,
    am.is_active,
    MAX(la.assigned_at) AS last_assigned_at,
    COUNT(la.id) AS lifetime_assignments
FROM agents_master am
LEFT JOIN lead_assignments la ON la.agent_id = am.id
GROUP BY am.id, am.agent_code, am.agent_name, am.is_active
ORDER BY am.is_active DESC, last_assigned_at DESC NULLS LAST, am.agent_code;
```

## 4) Eligible missed leads (pipeline input window)
Mirror the pipeline’s eligibility filter to see which leads will be picked up.

```sql
SELECT
    ml.pickup_row_id AS lead_id,
    ml.store_code,
    ml.customer_type,
    ml.lead_assigned,
    ml.is_order_placed,
    ml.pickup_created_date,
    ml.pickup_date,
    ml.run_date
FROM missed_leads ml
JOIN store_master sm ON sm.store_code = ml.store_code AND sm.assign_leads = true
JOIN store_lead_assignment_map slam ON slam.store_code = ml.store_code AND slam.is_enabled = true
JOIN agents_master am ON am.id = slam.agent_id AND am.is_active = true
WHERE ml.customer_type = 'New'
  AND ml.lead_assigned = false
  AND (ml.is_order_placed = false OR ml.is_order_placed IS NULL)
ORDER BY ml.pickup_created_date DESC, ml.pickup_row_id DESC;
```

## 5) Quota usage (per-day caps vs. today’s assignments)
Check whether today’s counts are near or over the configured caps.

```sql
WITH today AS (
    SELECT
        la.store_code,
        la.agent_id,
        SUM(CASE WHEN la.lead_type = 'E' THEN 1 ELSE 0 END) AS existing_count,
        SUM(CASE WHEN la.lead_type = 'N' THEN 1 ELSE 0 END) AS new_count,
        COUNT(*) AS total_count
    FROM lead_assignments la
    WHERE (la.lead_date = CURRENT_DATE OR CAST(la.assigned_at AS DATE) = CURRENT_DATE)
    GROUP BY la.store_code, la.agent_id
)
SELECT
    slam.store_code,
    am.agent_code,
    COALESCE(today.existing_count, 0) AS assigned_existing_today,
    COALESCE(today.new_count, 0) AS assigned_new_today,
    COALESCE(today.total_count, 0) AS assigned_total_today,
    slam.max_existing_per_lot,
    slam.max_new_per_lot,
    slam.max_daily_leads
FROM store_lead_assignment_map slam
JOIN agents_master am ON am.id = slam.agent_id
LEFT JOIN today ON today.store_code = slam.store_code AND today.agent_id = slam.agent_id
WHERE slam.is_enabled = true AND am.is_active = true
ORDER BY slam.store_code, am.agent_code;
```

## 6) Recent batches and assignments
Validate batch creation cadence and how many assignments each batch produced.

```sql
SELECT
    lab.id AS batch_id,
    lab.batch_date,
    lab.triggered_by,
    lab.run_id,
    lab.created_at,
    COUNT(la.id) AS assignment_count
FROM lead_assignment_batches lab
LEFT JOIN lead_assignments la ON la.assignment_batch_id = lab.id
GROUP BY lab.id, lab.batch_date, lab.triggered_by, lab.run_id, lab.created_at
ORDER BY lab.batch_date DESC, lab.id DESC
LIMIT 15;
```

## 7) Documents / PDF linkage
Ensure per-store/per-agent PDFs were emitted and registered in `documents`.

```sql
SELECT
    d.id,
    d.doc_date,
    d.reference_id_2 AS store_code,
    d.reference_id_3 AS agent_code,
    d.file_name,
    d.file_path,
    d.status,
    d.created_at
FROM documents d
WHERE d.doc_type = 'leads_assignment'
  AND d.doc_subtype = 'per_store_agent_pdf'
ORDER BY d.doc_date DESC, d.id DESC
LIMIT 25;
```

## 8) Notification profile, templates, and recipients
Confirm the notification profile, templates (default + summary), and store-level recipient rows.

```sql
-- Active profiles for the pipeline
SELECT id, code, env, scope, attach_mode, is_active
FROM notification_profiles
WHERE code = 'leads_assignment'
ORDER BY env;

-- Templates bound to the profile (default + summary)
SELECT et.profile_id, et.name, et.subject_template, et.body_template, et.is_active
FROM email_templates et
WHERE et.profile_id IN (
    SELECT id FROM notification_profiles WHERE code = 'leads_assignment'
)
ORDER BY et.profile_id, et.name;

-- Store-scoped recipients that will receive the PDFs
SELECT nr.profile_id, nr.store_code, nr.env, nr.email_address, nr.display_name, nr.send_as, nr.is_active
FROM notification_recipients nr
WHERE nr.profile_id IN (
    SELECT id FROM notification_profiles WHERE code = 'leads_assignment'
)
ORDER BY nr.store_code NULLS FIRST, nr.env, nr.send_as, nr.email_address;
```

## 9) SMTP configuration (email dispatch prerequisites)
Validate the SMTP credentials/host/port stored in `system_config`.

```sql
SELECT key, value, is_active
FROM system_config
WHERE key IN (
    'REPORT_EMAIL_FROM',
    'REPORT_EMAIL_SMTP_HOST',
    'REPORT_EMAIL_SMTP_PORT',
    'REPORT_EMAIL_SMTP_USERNAME',
    'REPORT_EMAIL_SMTP_PASSWORD',
    'REPORT_EMAIL_USE_TLS'
)
ORDER BY key;
```

## 10) Email dispatch audit (coverage vs. planned sends)
Reconcile which stores had PDFs for the latest batch and whether recipients exist for them.

```sql
WITH latest_batch AS (
    SELECT id, batch_date FROM lead_assignment_batches ORDER BY batch_date DESC, id DESC LIMIT 1
),
assignments AS (
    SELECT DISTINCT store_code, agent_id
    FROM lead_assignments
    WHERE assignment_batch_id = (SELECT id FROM latest_batch)
),
docs AS (
    SELECT DISTINCT reference_id_2 AS store_code, reference_id_3 AS agent_code
    FROM documents
    WHERE doc_type = 'leads_assignment'
      AND doc_subtype = 'per_store_agent_pdf'
      AND doc_date = (SELECT batch_date FROM latest_batch)
),
recipients AS (
    SELECT DISTINCT nr.store_code
    FROM notification_recipients nr
    WHERE nr.profile_id IN (SELECT id FROM notification_profiles WHERE code = 'leads_assignment')
      AND nr.is_active = true
),
global_recipients AS (
    SELECT 1 AS has_global
    FROM recipients
    WHERE store_code IS NULL
    LIMIT 1
)
SELECT
    a.store_code,
    a.agent_id,
    CASE WHEN d.store_code IS NOT NULL THEN 'yes' ELSE 'no' END AS has_document,
    CASE WHEN r.store_code IS NOT NULL OR g.has_global = 1 THEN 'yes' ELSE 'no' END AS has_any_recipient
FROM assignments a
LEFT JOIN docs d ON d.store_code = a.store_code
LEFT JOIN recipients r ON r.store_code = a.store_code
LEFT JOIN global_recipients g ON TRUE
ORDER BY a.store_code, a.agent_id;
```

> Tip: A `has_document = no` row indicates PDF generation/registration gaps. `has_any_recipient = no` means emails would be skipped for that store even if PDFs exist.
