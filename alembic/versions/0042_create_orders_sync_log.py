"""Create orders_sync_log table."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0042_create_orders_sync_log"
down_revision = "0041_skip_lead_assignment_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orders_sync_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("pipeline_id", sa.BigInteger(), sa.ForeignKey("pipelines.id"), nullable=False),
        sa.Column("run_id", sa.String(length=64), sa.ForeignKey("pipeline_run_summaries.run_id"), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("from_date", sa.Date(), nullable=False),
        sa.Column("to_date", sa.Date(), nullable=False),
        sa.Column("orders_pulled_at", sa.DateTime(timezone=True)),
        sa.Column("sales_pulled_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index(
        "uq_orders_sync_log_window",
        "orders_sync_log",
        ["pipeline_id", "store_code", "from_date", "to_date", "run_id"],
        unique=True,
    )
    op.create_index(
        "ix_orders_sync_log_lookup",
        "orders_sync_log",
        ["pipeline_id", "store_code", "from_date", "to_date", "status"],
    )
    op.create_index(
        "ix_orders_sync_log_store_recent",
        "orders_sync_log",
        ["store_code", sa.text("created_at DESC")],
    )
    op.create_index(
        "ix_orders_sync_log_pipeline_recent",
        "orders_sync_log",
        ["pipeline_id", sa.text("created_at DESC")],
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_orders_sync_log_updated_at()
        RETURNS trigger AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_orders_sync_log_updated_at
        BEFORE UPDATE ON orders_sync_log
        FOR EACH ROW
        EXECUTE FUNCTION set_orders_sync_log_updated_at();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_orders_sync_log_updated_at ON orders_sync_log")
    op.execute("DROP FUNCTION IF EXISTS set_orders_sync_log_updated_at")

    op.drop_index("ix_orders_sync_log_pipeline_recent", table_name="orders_sync_log")
    op.drop_index("ix_orders_sync_log_store_recent", table_name="orders_sync_log")
    op.drop_index("ix_orders_sync_log_lookup", table_name="orders_sync_log")
    op.drop_index("uq_orders_sync_log_window", table_name="orders_sync_log")
    op.drop_table("orders_sync_log")
