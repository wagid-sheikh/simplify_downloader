"""Create canonical orders reporting view."""

from __future__ import annotations

from alembic import op


revision = "0104_create_vw_orders"
down_revision = "0103_td_order_adjustment"
branch_labels = None
depends_on = None


ORDER_AMOUNT_EXPR = """
CASE
    WHEN (
        CASE
            WHEN COALESCE(o.adjustment, 0) > 0 THEN
                COALESCE(
                    CASE
                        WHEN o.source_system = 'TumbleDry'
                             AND o.net_amount IS NOT NULL
                             AND o.net_amount <> 0
                            THEN o.net_amount
                        WHEN o.source_system = 'TumbleDry'
                            THEN o.gross_amount
                        ELSE o.gross_amount
                    END,
                    0
                ) - COALESCE(o.adjustment, 0)
            ELSE
                COALESCE(
                    CASE
                        WHEN o.source_system = 'TumbleDry'
                             AND o.net_amount IS NOT NULL
                             AND o.net_amount <> 0
                            THEN o.net_amount
                        WHEN o.source_system = 'TumbleDry'
                            THEN o.gross_amount
                        ELSE o.gross_amount
                    END,
                    0
                )
        END
    ) <= 0 THEN 0
    ELSE (
        CASE
            WHEN COALESCE(o.adjustment, 0) > 0 THEN
                COALESCE(
                    CASE
                        WHEN o.source_system = 'TumbleDry'
                             AND o.net_amount IS NOT NULL
                             AND o.net_amount <> 0
                            THEN o.net_amount
                        WHEN o.source_system = 'TumbleDry'
                            THEN o.gross_amount
                        ELSE o.gross_amount
                    END,
                    0
                ) - COALESCE(o.adjustment, 0)
            ELSE
                COALESCE(
                    CASE
                        WHEN o.source_system = 'TumbleDry'
                             AND o.net_amount IS NOT NULL
                             AND o.net_amount <> 0
                            THEN o.net_amount
                        WHEN o.source_system = 'TumbleDry'
                            THEN o.gross_amount
                        ELSE o.gross_amount
                    END,
                    0
                )
        END
    )
END
"""


def _view_sql(*, create_or_replace: bool, schema_prefix: str) -> str:
    create_clause = "CREATE OR REPLACE VIEW" if create_or_replace else "CREATE VIEW"
    return f"""
-- Raw net_amount, gross_amount, and adjustment remain source/ingest fields
-- exposed unchanged from orders for lineage, reconciliation, and auditing.
-- order_amount is the only approved amount for reports, payment checks,
-- recovery checks, pending-delivery logic, and decision-making.
{create_clause} {schema_prefix}vw_orders AS
SELECT
    o.*,
    {ORDER_AMOUNT_EXPR} AS order_amount
FROM {schema_prefix}orders AS o;
"""


def upgrade() -> None:
    bind = op.get_bind()
    dialect_name = bind.dialect.name

    if dialect_name == "postgresql":
        op.execute(_view_sql(create_or_replace=True, schema_prefix="public."))
        op.execute(
            """
            COMMENT ON VIEW public.vw_orders IS
            'Canonical reporting and decision-making view over orders. Raw net_amount, gross_amount, and adjustment remain source/ingest fields; order_amount is the approved amount for reports, payment checks, recovery checks, pending-delivery logic, and decision-making.';
            """
        )
        op.execute(
            """
            COMMENT ON COLUMN public.vw_orders.order_amount IS
            'Approved derived order amount after source-aware base amount selection and positive adjustment reduction.';
            """
        )
    else:
        op.execute("DROP VIEW IF EXISTS vw_orders;")
        op.execute(_view_sql(create_or_replace=False, schema_prefix=""))


def downgrade() -> None:
    # Forward-only migration: keep the canonical orders view in place.
    pass
