"""Recompute sales.is_edited_order flags using corrected rules."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0061_recompute_sales_is_edited_order"
down_revision = "0060_add_missed_leadged_recipint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Recompute edited-order flags based on corrected payment/duplicate logic.

    Missing orders are treated as not-underpaid; for sales rows without a matching
    orders record, only duplicate payment-mode logic applies, otherwise the flag
    is cleared.
    """
    op.execute(
        sa.text(
            """
            WITH sales_totals AS (
                SELECT
                    store_code,
                    order_number,
                    COALESCE(SUM(payment_received), 0) AS total_payment
                FROM sales
                GROUP BY store_code, order_number
            ),
            underpaid_orders AS (
                SELECT
                    totals.store_code,
                    totals.order_number
                FROM sales_totals totals
                JOIN orders
                    ON orders.store_code = totals.store_code
                    AND orders.order_number = totals.order_number
                WHERE totals.total_payment < orders.gross_amount
            ),
            duplicate_payment_modes AS (
                SELECT
                    store_code,
                    order_number,
                    payment_mode
                FROM sales
                GROUP BY store_code, order_number, payment_mode
                HAVING COUNT(*) > 1
            ),
            flags AS (
                SELECT
                    sales.id,
                    underpaid.store_code IS NOT NULL AS is_underpaid,
                    duplicates.store_code IS NOT NULL AS is_duplicate
                FROM sales
                LEFT JOIN underpaid_orders AS underpaid
                    ON underpaid.store_code = sales.store_code
                    AND underpaid.order_number = sales.order_number
                LEFT JOIN duplicate_payment_modes AS duplicates
                    ON duplicates.store_code = sales.store_code
                    AND duplicates.order_number = sales.order_number
                    AND duplicates.payment_mode = sales.payment_mode
            )
            UPDATE sales
            SET is_edited_order = (flags.is_underpaid OR flags.is_duplicate)
            FROM flags
            WHERE sales.id = flags.id
                AND sales.is_edited_order
                    IS DISTINCT FROM (flags.is_underpaid OR flags.is_duplicate)
            """
        )
    )


def downgrade() -> None:
    """No-op: previous edited-order flags cannot be reconstructed."""
    return
