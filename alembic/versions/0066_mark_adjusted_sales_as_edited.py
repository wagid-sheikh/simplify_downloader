"""Mark sales rows with adjustments as edited and add remarks."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0066_mark_adjusted_sales_as_edited"
down_revision = "0065_add_timeout_config_keys"
branch_labels = None
depends_on = None


ADJUSTMENT_REMARK = "Orders Value was adjusted"


def _update_adjusted_rows(table_name: str) -> None:
    connection = op.get_bind()
    connection.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET is_edited_order = TRUE,
                ingest_remarks = CASE
                    WHEN ingest_remarks IS NULL OR ingest_remarks = '' THEN :remark
                    WHEN ingest_remarks LIKE '%' || :remark || '%' THEN ingest_remarks
                    ELSE ingest_remarks || '; ' || :remark
                END
            WHERE adjustments > 0
                AND (
                    is_edited_order IS DISTINCT FROM TRUE
                    OR ingest_remarks IS NULL
                    OR ingest_remarks NOT LIKE '%' || :remark || '%'
                )
            """
        ),
        {"remark": ADJUSTMENT_REMARK},
    )


def upgrade() -> None:
    """Mark adjusted sales rows as edited and append remarks."""
    _update_adjusted_rows("sales")
    _update_adjusted_rows("stg_td_sales")


def downgrade() -> None:
    """No-op: previous edited-order flags cannot be reconstructed."""
    return
