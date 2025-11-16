"""Add notification tables and seed email data."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_notification_tables"
down_revision = "0006_documents"
branch_labels = None
depends_on = None

pipelines_table = sa.Table(
    "pipelines",
    sa.MetaData(),
    sa.Column("id", sa.BigInteger()),
    sa.Column("code", sa.Text()),
    sa.Column("description", sa.Text()),
)

notification_profiles_table = sa.Table(
    "notification_profiles",
    sa.MetaData(),
    sa.Column("id", sa.BigInteger()),
    sa.Column("pipeline_id", sa.BigInteger()),
    sa.Column("code", sa.Text()),
    sa.Column("description", sa.Text()),
    sa.Column("env", sa.Text()),
    sa.Column("scope", sa.Text()),
    sa.Column("attach_mode", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
)

email_templates_table = sa.Table(
    "email_templates",
    sa.MetaData(),
    sa.Column("id", sa.BigInteger()),
    sa.Column("profile_id", sa.BigInteger()),
    sa.Column("name", sa.Text()),
    sa.Column("subject_template", sa.Text()),
    sa.Column("body_template", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
)

notification_recipients_table = sa.Table(
    "notification_recipients",
    sa.MetaData(),
    sa.Column("id", sa.BigInteger()),
    sa.Column("profile_id", sa.BigInteger()),
    sa.Column("store_code", sa.Text()),
    sa.Column("env", sa.Text()),
    sa.Column("email_address", sa.Text()),
    sa.Column("display_name", sa.Text()),
    sa.Column("send_as", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
)


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("code", sa.Text(), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=False),
    )

    op.create_table(
        "notification_profiles",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("pipeline_id", sa.BigInteger(), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("env", sa.Text(), nullable=False, server_default="any"),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("attach_mode", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.UniqueConstraint("pipeline_id", "code", "env", name="uq_notification_profiles_pipeline_code_env"),
        sa.CheckConstraint("scope IN ('run','store','combined')", name="ck_notification_profiles_scope"),
        sa.CheckConstraint(
            "attach_mode IN ('none','per_store_pdf','all_store_pdfs','all_docs_for_run')",
            name="ck_notification_profiles_attach_mode",
        ),
        sa.CheckConstraint("env IN ('dev','prod','local','any')", name="ck_notification_profiles_env"),
    )

    op.create_table(
        "email_templates",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "profile_id",
            sa.BigInteger(),
            sa.ForeignKey("notification_profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("subject_template", sa.Text(), nullable=False),
        sa.Column("body_template", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.UniqueConstraint("profile_id", "name", name="uq_email_templates_profile_name"),
    )

    op.create_table(
        "notification_recipients",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "profile_id",
            sa.BigInteger(),
            sa.ForeignKey("notification_profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("store_code", sa.Text(), nullable=True),
        sa.Column("env", sa.Text(), nullable=False, server_default="any"),
        sa.Column("email_address", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("send_as", sa.Text(), nullable=False, server_default="to"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("send_as IN ('to','cc','bcc')", name="ck_notification_recipients_send_as"),
        sa.CheckConstraint("env IN ('dev','prod','local','any')", name="ck_notification_recipients_env"),
    )

    connection = op.get_bind()

    pipeline_rows = [
        {"code": "simplify_dashboard_daily", "description": "Daily single-session dashboard pipeline"},
        {"code": "simplify_dashboard_weekly", "description": "Weekly dashboard reporting pipeline"},
        {"code": "simplify_dashboard_monthly", "description": "Monthly dashboard reporting pipeline"},
        {"code": "crm_downloader_daily", "description": "Daily CRM downloader pipeline"},
    ]

    pipeline_ids: dict[str, int] = {}
    for row in pipeline_rows:
        result = connection.execute(
            pipelines_table.insert().values(**row).returning(pipelines_table.c.id)
        )
        pipeline_ids[row["code"]] = result.scalar_one()

    run_summary_recipients = [
        {"send_as": "to", "email_address": "wagid.sheikh@gmail.com"},
        {"send_as": "cc", "email_address": "sheikhhabib29@gmail.com"},
        {"send_as": "cc", "email_address": "adnanalisheikhmgmnt@gmail.com"},
    ]

    store_recipients = {
        "A668": [
            {"send_as": "to", "email_address": "tumbledry.uttamnagardelhi@gmail.com"},
            {"send_as": "cc", "email_address": "shaw@theshawventures.com"},
            {"send_as": "cc", "email_address": "wagid.sheikh@gmail.com"},
            {"send_as": "cc", "email_address": "sheikhhabib29@gmail.com"},
            {"send_as": "cc", "email_address": "adnanalisheikhmgmnt@gmail.com"},
            {"send_as": "cc", "email_address": "theshawventures@gmail.com"},
        ],
        "A817": [
            {"send_as": "to", "email_address": "td.kirtinagar@gmail.com"},
            {"send_as": "cc", "email_address": "shaw@theshawventures.com"},
            {"send_as": "cc", "email_address": "wagid.sheikh@gmail.com"},
            {"send_as": "cc", "email_address": "sheikhhabib29@gmail.com"},
            {"send_as": "cc", "email_address": "adnanalisheikhmgmnt@gmail.com"},
            {"send_as": "cc", "email_address": "theshawventures@gmail.com"},
        ],
    }

    def insert_profile(
        *,
        pipeline_code: str,
        code: str,
        description: str,
        scope: str,
        attach_mode: str,
    ) -> int:
        result = connection.execute(
            notification_profiles_table.insert()
            .values(
                pipeline_id=pipeline_ids[pipeline_code],
                code=code,
                description=description,
                env="any",
                scope=scope,
                attach_mode=attach_mode,
                is_active=True,
            )
            .returning(notification_profiles_table.c.id)
        )
        return result.scalar_one()

    def insert_template(profile_id: int, subject: str, body: str) -> None:
        connection.execute(
            email_templates_table.insert().values(
                profile_id=profile_id,
                name="default",
                subject_template=subject,
                body_template=body,
                is_active=True,
            )
        )

    def insert_recipients(profile_id: int, rows: list[dict[str, str]], *, store_code: str | None = None) -> None:
        payload = []
        for row in rows:
            payload.append(
                {
                    "profile_id": profile_id,
                    "store_code": store_code,
                    "env": "any",
                    "email_address": row["email_address"],
                    "display_name": None,
                    "send_as": row["send_as"],
                    "is_active": True,
                }
            )
        if payload:
            connection.execute(notification_recipients_table.insert(), payload)

    run_summary_subject = "[TSV Pipeline Run] {{ pipeline_name }} | {{ report_date }}"
    run_summary_body = "{{ summary_text }}\n\n(Env: {{ run_env }}, Run ID: {{ run_id }})"

    for code in pipeline_ids:
        profile_id = insert_profile(
            pipeline_code=code,
            code="run_summary",
            description="Run summary (pipeline health) for this pipeline",
            scope="run",
            attach_mode="none",
        )
        insert_template(profile_id, run_summary_subject, run_summary_body)
        insert_recipients(profile_id, run_summary_recipients)

    daily_profile_id = insert_profile(
        pipeline_code="simplify_dashboard_daily",
        code="store_daily_reports",
        description="Daily store performance PDFs per store",
        scope="store",
        attach_mode="per_store_pdf",
    )
    insert_template(
        daily_profile_id,
        "[TSV Store Performance] {{ store_code }} | {{ report_date }}",
        "Attached: daily performance report for store {{ store_code }} on {{ report_date }}.",
    )
    for store_code, recipients in store_recipients.items():
        insert_recipients(daily_profile_id, recipients, store_code=store_code)

    weekly_profile_id = insert_profile(
        pipeline_code="simplify_dashboard_weekly",
        code="store_weekly_reports",
        description="Weekly store performance PDFs per store",
        scope="store",
        attach_mode="per_store_pdf",
    )
    insert_template(
        weekly_profile_id,
        "[TSV Store Performance - Weekly] {{ store_code }} | Week ending {{ report_date }}",
        "Attached: weekly performance report for store {{ store_code }} (week ending {{ report_date }}).",
    )
    for store_code, recipients in store_recipients.items():
        insert_recipients(weekly_profile_id, recipients, store_code=store_code)

    monthly_profile_id = insert_profile(
        pipeline_code="simplify_dashboard_monthly",
        code="store_monthly_reports",
        description="Monthly store performance PDFs per store",
        scope="store",
        attach_mode="per_store_pdf",
    )
    insert_template(
        monthly_profile_id,
        "[TSV Store Performance - Monthly] {{ store_code }} | {{ report_date }}",
        "Attached: monthly performance report for store {{ store_code }} (month ending {{ report_date }}).",
    )
    for store_code, recipients in store_recipients.items():
        insert_recipients(monthly_profile_id, recipients, store_code=store_code)


def downgrade() -> None:
    op.drop_table("notification_recipients")
    op.drop_table("email_templates")
    op.drop_table("notification_profiles")
    op.drop_table("pipelines")
