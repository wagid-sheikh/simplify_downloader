"""Align TD/UC/bank keys, constraints, and notification seeds.

Order of operations (staging keys → production keys → seeds):
- Enforce staging upsert keys for td_orders, td_sales, uc_orders, and bank.
- Mirror production keys for orders, td_sales, and bank to keep ETL idempotent.
- Seed notification pipelines/profiles/templates for td_orders_sync, uc_orders_sync, and bank_sync.
"""

from __future__ import annotations

from collections.abc import Iterable

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0024_align_td_uc_bank_keys_seeds"
down_revision = "0023_leads_assignment_templates"
branch_labels = None
depends_on = None

# Upsert key documentation for ETL reference:
# - stg_td_orders + td_orders: (store_code, order_number, order_date) / (cost_center, order_number, order_date)
# - stg_td_sales + td_sales: (store_code, order_number, payment_date) / (cost_center, order_number, payment_date)
# - stg_uc_orders + uc_orders: (store_code, order_number, invoice_date) / (cost_center, order_number, invoice_date)
# - stg_bank + bank: (row_id)


STAGING_UNIQUE_SPECS: list[tuple[str, list[str], str]] = [
    ("stg_td_orders", ["store_code", "order_number", "order_date"], "uq_stg_td_orders_store_order_date"),
    ("stg_td_sales", ["store_code", "order_number", "payment_date"], "uq_stg_td_sales_store_order_payment_date"),
    ("stg_uc_orders", ["store_code", "order_number", "invoice_date"], "uq_stg_uc_orders_store_order_invoice_date"),
    ("stg_bank", ["row_id"], "uq_stg_bank_row_id"),
]

PRODUCTION_UNIQUE_SPECS: list[tuple[str, list[str], str, list[list[str]]]] = [
    (
        "orders",
        ["cost_center", "order_number", "order_date"],
        "uq_orders_cost_center_order_number_order_date",
        [["cost_center", "order_number"]],
    ),
    (
        "td_sales",
        ["cost_center", "order_number", "payment_date"],
        "uq_td_sales_cost_center_order_number_payment_date",
        [["cost_center", "order_number"]],
    ),
    ("uc_orders", ["cost_center", "order_number", "invoice_date"], "uq_uc_orders_cost_center_order_invoice_date", []),
    ("bank", ["row_id"], "uq_bank_row_id", [["row_id"]]),
]

PIPELINE_SEEDS = {
    "td_orders_sync": "TD Orders Sync Pipeline",
    "uc_orders_sync": "UC Orders Sync Pipeline",
    "bank_sync": "Bank Sync Pipeline",
}

PROFILE_SCOPE = "run"
PROFILE_ATTACH_MODE = "none"
PROFILE_ENV = "any"
PROFILE_CODE = "default"

TEMPLATE_SUBJECTS = {
    "td_orders_sync": "TD Orders Sync – {{ overall_status }}",
    "uc_orders_sync": "UC Orders Sync – {{ overall_status }}",
    "bank_sync": "Bank Sync – {{ overall_status }}",
}

TEMPLATE_BODIES = {
    "td_orders_sync": """
Run ID: {{ run_id }} | Overall Status: {{ overall_status }}
Started: {{ started_at }} | Finished: {{ finished_at }}

{{ summary_text }}

{% if overall_status == 'ok' %}
All TD stores completed successfully. Proceed with merge to production using (cost_center, order_number, order_date).
{% elif overall_status == 'warning' %}
Mixed TD outcomes: review failed stores above, re-run after fixing source data, and rely on the unique business key to avoid duplicates.
{% else %}
All TD stores failed. Check error summaries and retry; no production rows were updated due to the enforced unique constraint.
{% endif %}
""",
    "uc_orders_sync": """
UC Orders Sync Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}

**Per Store UC Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.status or 'unknown')|upper }}
  filename: {{ store.filename or 'n/a' }}
  staging_inserted: {{ store.staging_inserted or 0 }}
  staging_updated: {{ store.staging_updated or 0 }}
  final_inserted: {{ store.final_inserted or 0 }}
  final_updated: {{ store.final_updated or 0 }}
  warning_count: {{ store.warning_count or 0 }}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status in ['ok', 'success'] %}
All UC stores completed successfully. Upsert using (cost_center, order_number, invoice_date) to keep reruns idempotent.
{% elif overall_status in ['warning', 'partial', 'skipped'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Mixed UC outcomes: review warning/error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}
{% else %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
UC sync failed after mixed store outcomes. Review failures above and retry once resolved.
{% endif %}
{% endif %}
""",
    "bank_sync": """
Run ID: {{ run_id }} | Overall Status: {{ overall_status }}
Started: {{ started_at }} | Finished: {{ finished_at }}

Bank file status:
{% for file in files %}
- {{ file.file_name }}: {{ file.status }} | rows: {{ file.row_count }}{% if file.error_message %} | error: {{ file.error_message }}{% endif %}
{% endfor %}

{% if overall_status == 'ok' %}
All bank files processed successfully; upserts are keyed on row_id for safe retries.
{% elif overall_status == 'warning' %}
Partial bank success. Investigate failed files above; successful rows remain unique on row_id.
{% else %}
Bank sync failed. Check error summaries and rerun once resolved.
{% endif %}
""",
}

RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"


def _matching_columns(candidate: Iterable[str] | None, expected: Iterable[str]) -> bool:
    if candidate is None:
        return False
    return set(candidate) == set(expected)


def _drop_conflicting_uniques(
    inspector: sa.inspection.Inspector,
    *,
    table: str,
    expected_columns: list[str],
    conflicting_sets: list[list[str]],
) -> None:
    for constraint in inspector.get_unique_constraints(table):
        if _matching_columns(constraint.get("column_names"), expected_columns):
            continue
        if any(_matching_columns(constraint.get("column_names"), columns) for columns in conflicting_sets):
            op.drop_constraint(constraint["name"], table, type_="unique")

    for index in inspector.get_indexes(table):
        if not index.get("unique"):
            continue
        if _matching_columns(index.get("column_names"), expected_columns):
            continue
        if any(_matching_columns(index.get("column_names"), columns) for columns in conflicting_sets):
            op.drop_index(index["name"], table_name=table)


def _ensure_unique_constraint(
    inspector: sa.inspection.Inspector, *, table: str, columns: list[str], name: str
) -> str | None:
    if not inspector.has_table(table):
        return None

    for constraint in inspector.get_unique_constraints(table):
        if _matching_columns(constraint.get("column_names"), columns):
            return constraint["name"]

    for index in inspector.get_indexes(table):
        if index.get("unique") and _matching_columns(index.get("column_names"), columns):
            return index["name"]

    op.create_unique_constraint(name, table, columns)
    return name


def _pipeline_table() -> sa.Table:
    return sa.table(
        "pipelines",
        sa.column("id"),
        sa.column("code"),
        sa.column("description"),
    )


def _notification_profiles_table() -> sa.Table:
    return sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("description"),
        sa.column("env"),
        sa.column("scope"),
        sa.column("attach_mode"),
        sa.column("is_active"),
    )


def _email_templates_table() -> sa.Table:
    return sa.table(
        "email_templates",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("subject_template"),
        sa.column("body_template"),
        sa.column("is_active"),
    )


def _notification_recipients_table() -> sa.Table:
    return sa.table(
        "notification_recipients",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("store_code"),
        sa.column("env"),
        sa.column("email_address"),
        sa.column("display_name"),
        sa.column("send_as"),
        sa.column("is_active"),
    )


def _upsert_pipeline(connection, *, code: str, description: str) -> int:
    pipelines = _pipeline_table()
    stmt = (
        postgresql.insert(pipelines)
        .values(code=code, description=description)
        .on_conflict_do_update(index_elements=[pipelines.c.code], set_={"description": description})
        .returning(pipelines.c.id)
    )
    result = connection.execute(stmt)
    return result.scalar_one()


def _upsert_profile(connection, *, pipeline_id: int) -> int:
    profiles = _notification_profiles_table()
    stmt = (
        postgresql.insert(profiles)
        .values(
            pipeline_id=pipeline_id,
            code=PROFILE_CODE,
            description="Default status notifications",
            env=PROFILE_ENV,
            scope=PROFILE_SCOPE,
            attach_mode=PROFILE_ATTACH_MODE,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[profiles.c.pipeline_id, profiles.c.code, profiles.c.env],
            set_={
                "description": "Default status notifications",
                "scope": PROFILE_SCOPE,
                "attach_mode": PROFILE_ATTACH_MODE,
                "is_active": True,
            },
        )
        .returning(profiles.c.id)
    )
    result = connection.execute(stmt)
    return result.scalar_one()


def _upsert_template(connection, *, profile_id: int, subject: str, body: str) -> None:
    templates = _email_templates_table()
    stmt = (
        postgresql.insert(templates)
        .values(
            profile_id=profile_id,
            name="default",
            subject_template=subject,
            body_template=body,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[templates.c.profile_id, templates.c.name],
            set_={"subject_template": subject, "body_template": body, "is_active": True},
        )
    )
    connection.execute(stmt)


def _ensure_recipient(connection, *, profile_id: int) -> None:
    recipients = _notification_recipients_table()
    existing_id = connection.execute(
        sa.select(recipients.c.id)
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.env == PROFILE_ENV)
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == "to")
    ).scalar()

    payload = {
        "profile_id": profile_id,
        "store_code": None,
        "env": PROFILE_ENV,
        "email_address": RECIPIENT_EMAIL,
        "display_name": None,
        "send_as": "to",
        "is_active": True,
    }

    if existing_id:
        connection.execute(
            recipients.update().where(recipients.c.id == existing_id).values(payload)
        )
    else:
        connection.execute(recipients.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()

    # Staging constraints to protect ingestion upsert keys.
    for table, columns, name in STAGING_UNIQUE_SPECS:
        inspector = sa.inspect(connection)
        if not inspector.has_table(table):
            continue

        _ensure_unique_constraint(inspector, table=table, columns=columns, name=name)

    # Production constraints mirror staging to keep reruns idempotent.
    for table, columns, name, conflicting_sets in PRODUCTION_UNIQUE_SPECS:
        inspector = sa.inspect(connection)
        if not inspector.has_table(table):
            continue

        _drop_conflicting_uniques(
            inspector,
            table=table,
            expected_columns=columns,
            conflicting_sets=conflicting_sets,
        )
        inspector = sa.inspect(connection)
        _ensure_unique_constraint(inspector, table=table, columns=columns, name=name)

    # Notification seed data for TD/UC/bank sync pipelines.
    for code, description in PIPELINE_SEEDS.items():
        pipeline_id = _upsert_pipeline(connection, code=code, description=description)
        profile_id = _upsert_profile(connection, pipeline_id=pipeline_id)
        _upsert_template(
            connection,
            profile_id=profile_id,
            subject=TEMPLATE_SUBJECTS[code],
            body=TEMPLATE_BODIES[code],
        )
        _ensure_recipient(connection, profile_id=profile_id)


def downgrade() -> None:
    connection = op.get_bind()

    # Remove seeded recipients/templates/profiles for the target pipelines.
    recipients = _notification_recipients_table()
    templates = _email_templates_table()
    profiles = _notification_profiles_table()
    pipelines = _pipeline_table()

    pipeline_ids = [
        row["id"]
        for row in connection.execute(
            sa.select(pipelines.c.id).where(pipelines.c.code.in_(PIPELINE_SEEDS.keys()))
        ).mappings()
    ]

    if pipeline_ids:
        connection.execute(
            recipients.delete()
            .where(recipients.c.profile_id.in_(sa.select(profiles.c.id).where(profiles.c.pipeline_id.in_(pipeline_ids))))
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
            .where(recipients.c.env == PROFILE_ENV)
        )

        connection.execute(
            templates.delete()
            .where(templates.c.profile_id.in_(sa.select(profiles.c.id).where(profiles.c.pipeline_id.in_(pipeline_ids))))
            .where(templates.c.name == "default")
            .where(templates.c.subject_template.in_(TEMPLATE_SUBJECTS.values()))
        )

        connection.execute(
            profiles.delete()
            .where(profiles.c.pipeline_id.in_(pipeline_ids))
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.env == PROFILE_ENV)
        )

        connection.execute(pipelines.delete().where(pipelines.c.id.in_(pipeline_ids)))

    # Drop constraints created by this migration (do not drop pre-existing ones).
    for table, columns, name in STAGING_UNIQUE_SPECS:
        inspector = sa.inspect(connection)
        if not inspector.has_table(table):
            continue
        for constraint in inspector.get_unique_constraints(table):
            if constraint["name"] == name:
                op.drop_constraint(name, table, type_="unique")
                break
        else:
            for index in inspector.get_indexes(table):
                if index["name"] == name:
                    op.drop_index(name, table_name=table)
                    break

    for table, columns, name, _ in PRODUCTION_UNIQUE_SPECS:
        inspector = sa.inspect(connection)
        if not inspector.has_table(table):
            continue
        for constraint in inspector.get_unique_constraints(table):
            if constraint["name"] == name:
                op.drop_constraint(name, table, type_="unique")
                break
        else:
            for index in inspector.get_indexes(table):
                if index["name"] == name:
                    op.drop_index(name, table_name=table)
                    break
