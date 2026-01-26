"""Clear edited-order flags caused by overlapping sales ingests."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0064_clear_overlap_sales_edits"
down_revision = "0063_seed_pending_deliveries_rep"
branch_labels = None
depends_on = None


def _clear_overlap_flags(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET is_edited_order = FALSE,
                ingest_remarks = NULLIF(
                    TRIM(
                        BOTH '; ' FROM regexp_replace(
                            ingest_remarks,
                            '(; )?Order already exists in sales data for payment_date ''[^'']+''',
                            '',
                            'g'
                        )
                    ),
                    ''
                )
            WHERE ingest_remarks LIKE '%Order already exists in sales data for payment_date%'
              AND ingest_remarks NOT LIKE '%Duplicate order_number/payment_mode%'
              AND ingest_remarks NOT LIKE '%Total payment_received%'
            """
        )
    )


def upgrade() -> None:
    _clear_overlap_flags("sales")
    _clear_overlap_flags("stg_td_sales")


def downgrade() -> None:
    """No-op: cleaned flags cannot be reconstructed."""
    return
