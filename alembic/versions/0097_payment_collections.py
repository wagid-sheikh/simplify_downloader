"""Create payment_collections table for manual payment transaction capture."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0097_payment_collections"
down_revision = "0096_seed_mtd_same_day_notif"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payment_collections",
        sa.Column("payment_id", sa.BigInteger(), sa.Identity(always=False), primary_key=True, nullable=False),
        sa.Column("source_sheet_row", sa.Integer(), nullable=False, unique=True),
        sa.Column("payment_timestamp", sa.DateTime(timezone=False), nullable=False),
        sa.Column("email_address", sa.Text(), nullable=True),
        sa.Column("payment_mode", sa.Text(), nullable=False),
        sa.Column("store_code", sa.String(length=30), nullable=False),
        sa.Column("payment_date", sa.Date(), nullable=False),
        sa.Column("order_number", sa.String(length=50), nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("source_rowid", sa.String(length=50), nullable=True),
        sa.Column("handed_over", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("date_handed", sa.Date(), nullable=True),
        sa.Column("date_modified", sa.Date(), nullable=True),
        sa.Column("updated_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("amount >= 0", name="ck_payment_collections_amount_nonneg"),
    )

    op.create_index(
        "idx_payment_collections_store_date",
        "payment_collections",
        ["store_code", "payment_date"],
        unique=False,
    )
    op.create_index(
        "idx_payment_collections_order_number",
        "payment_collections",
        ["order_number"],
        unique=False,
    )
    op.create_index(
        "idx_payment_collections_mode",
        "payment_collections",
        ["payment_mode"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_payment_collections_mode", table_name="payment_collections")
    op.drop_index("idx_payment_collections_order_number", table_name="payment_collections")
    op.drop_index("idx_payment_collections_store_date", table_name="payment_collections")
    op.drop_table("payment_collections")
