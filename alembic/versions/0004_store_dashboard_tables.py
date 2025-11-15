"""Create tables for store dashboard summaries."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_store_dashboard"
down_revision = "0003_undelivered_pk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "store_master",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("store_code", sa.Text(), nullable=False, unique=True),
        sa.Column("store_name", sa.Text(), nullable=True),
        sa.Column("gstin", sa.Text(), nullable=True),
        sa.Column("launch_date", sa.Date(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "store_dashboard_summary",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "store_id",
            sa.BigInteger(),
            sa.ForeignKey("store_master.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dashboard_date", sa.Date(), nullable=False),
        sa.Column("run_date_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("prev_month_revenue", sa.Numeric(14, 2), nullable=True),
        sa.Column("target_revenue", sa.Numeric(14, 2), nullable=True),
        sa.Column("lmt_d_revenue", sa.Numeric(14, 2), nullable=True),
        sa.Column("mtd_revenue", sa.Numeric(14, 2), nullable=True),
        sa.Column("ftd_revenue", sa.Numeric(14, 2), nullable=True),
        sa.Column("tgt_vs_ach_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("growth_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("extrapolated_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("pickup_new_count", sa.Integer(), nullable=True),
        sa.Column("pickup_new_conv_count", sa.Integer(), nullable=True),
        sa.Column("pickup_new_conv_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("pickup_existing_count", sa.Integer(), nullable=True),
        sa.Column("pickup_existing_conv_count", sa.Integer(), nullable=True),
        sa.Column("pickup_existing_conv_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("pickup_total_count", sa.Integer(), nullable=True),
        sa.Column("pickup_total_conv_count", sa.Integer(), nullable=True),
        sa.Column("pickup_total_conv_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("delivery_total_orders", sa.Integer(), nullable=True),
        sa.Column("delivery_within_tat_count", sa.Integer(), nullable=True),
        sa.Column("delivery_tat_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("delivery_total_delivered", sa.Integer(), nullable=True),
        sa.Column("delivery_undel_over_10_days", sa.Integer(), nullable=True),
        sa.Column("delivery_total_undelivered", sa.Integer(), nullable=True),
        sa.Column("repeat_customer_base_6m", sa.Integer(), nullable=True),
        sa.Column("repeat_orders", sa.Integer(), nullable=True),
        sa.Column("repeat_total_base_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("package_target", sa.Integer(), nullable=True),
        sa.Column("package_new", sa.Integer(), nullable=True),
        sa.Column("package_ftd", sa.Integer(), nullable=True),
        sa.Column("package_achievement_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("package_overall", sa.Integer(), nullable=True),
        sa.Column("package_non_pkg_over_800", sa.Integer(), nullable=True),
        sa.Column(
            "package_non_pkg_over_800_undelivered",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "store_id",
            "dashboard_date",
            name="uq_store_dashboard_summary_store_date",
        ),
    )


def downgrade() -> None:
    op.drop_table("store_dashboard_summary")
    op.drop_table("store_master")
