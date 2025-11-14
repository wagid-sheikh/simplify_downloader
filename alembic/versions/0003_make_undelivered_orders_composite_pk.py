"""Make undelivered_orders use a composite primary key"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0003_undelivered_pk"
down_revision = "0002_undelivered_uc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE undelivered_orders
        DROP CONSTRAINT IF EXISTS uq_undelivered_orders_store_code_order_id;
        """
    )
    op.execute(
        """
        ALTER TABLE undelivered_orders
        DROP CONSTRAINT IF EXISTS uq_undelivered_order_id;
        """
    )
    op.execute(
        """
        ALTER TABLE undelivered_orders
        DROP CONSTRAINT IF EXISTS undelivered_orders_pkey;
        """
    )
    op.execute(
        """
        ALTER TABLE undelivered_orders
        ALTER COLUMN order_id SET NOT NULL;
        """
    )
    op.execute(
        """
        ALTER TABLE undelivered_orders
        ALTER COLUMN store_code SET NOT NULL;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_schema = current_schema()
                  AND table_name = 'undelivered_orders'
                  AND constraint_name = 'pk_undelivered_order'
            ) THEN
                ALTER TABLE undelivered_orders
                ADD CONSTRAINT pk_undelivered_order PRIMARY KEY (order_id, store_code);
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_schema = current_schema()
                  AND table_name = 'undelivered_orders'
                  AND constraint_name = 'pk_undelivered_order'
            ) THEN
                ALTER TABLE undelivered_orders
                DROP CONSTRAINT pk_undelivered_order;
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        ALTER TABLE undelivered_orders
        ALTER COLUMN store_code DROP NOT NULL;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_schema = current_schema()
                  AND table_name = 'undelivered_orders'
                  AND constraint_name = 'undelivered_orders_pkey'
            ) THEN
                ALTER TABLE undelivered_orders
                ADD CONSTRAINT undelivered_orders_pkey PRIMARY KEY (order_id);
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_schema = current_schema()
                  AND table_name = 'undelivered_orders'
                  AND constraint_name = 'uq_undelivered_order_id'
            ) THEN
                ALTER TABLE undelivered_orders
                ADD CONSTRAINT uq_undelivered_order_id UNIQUE (order_id);
            END IF;
        END
        $$;
        """
    )
