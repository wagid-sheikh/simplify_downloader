"""Track payment collection source systems."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0107_payment_coll_sources"
down_revision = "0106_recovery_categories"
branch_labels = None
depends_on = None



def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return index_name in {index["name"] for index in inspector.get_indexes(table_name)}


def _upgrade_postgresql() -> None:
    op.execute("ALTER TABLE public.payment_collections ADD COLUMN IF NOT EXISTS bank_row_id text;")
    op.execute("ALTER TABLE public.payment_collections ADD COLUMN IF NOT EXISTS source_type text;")
    op.execute(
        """
        UPDATE public.payment_collections
        SET source_type = 'google_sheet'
        WHERE source_type IS NULL;
        """
    )
    op.execute("ALTER TABLE public.payment_collections ALTER COLUMN source_type SET NOT NULL;")

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.payment_collections'::regclass
                  AND conname = 'ck_payment_collections_amount_nonneg'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.payment_collections'::regclass
                  AND conname = 'payment_collections_amount_check'
            ) THEN
                ALTER TABLE public.payment_collections
                RENAME CONSTRAINT ck_payment_collections_amount_nonneg
                TO payment_collections_amount_check;
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.payment_collections'::regclass
                  AND conname = 'chk_payment_collections_source_type'
            ) THEN
                ALTER TABLE public.payment_collections
                ADD CONSTRAINT chk_payment_collections_source_type
                CHECK (source_type = ANY (ARRAY['google_sheet'::text, 'legacy_sales'::text]));
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.payment_collections'::regclass
                  AND conname = 'uq_payment_collections_source_type_row'
            ) THEN
                ALTER TABLE public.payment_collections
                ADD CONSTRAINT uq_payment_collections_source_type_row
                UNIQUE (source_type, source_sheet_row);
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            constraint_record record;
        BEGIN
            FOR constraint_record IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'public.payment_collections'::regclass
                  AND contype = 'u'
                  AND conname <> 'uq_payment_collections_source_type_row'
                  AND conkey = ARRAY[
                      (SELECT attnum
                       FROM pg_attribute
                       WHERE attrelid = 'public.payment_collections'::regclass
                         AND attname = 'source_sheet_row')
                  ]::smallint[]
            LOOP
                EXECUTE format(
                    'ALTER TABLE public.payment_collections DROP CONSTRAINT %I',
                    constraint_record.conname
                );
            END LOOP;
        END $$;
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payment_collections_bank_row_id
        ON public.payment_collections (bank_row_id);
        """
    )


def _upgrade_generic() -> None:
    if not _has_column("payment_collections", "bank_row_id"):
        op.add_column("payment_collections", sa.Column("bank_row_id", sa.Text(), nullable=True))

    if not _has_column("payment_collections", "source_type"):
        op.add_column(
            "payment_collections",
            sa.Column(
                "source_type",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'google_sheet'"),
            ),
        )

    if not _has_index("payment_collections", "idx_payment_collections_bank_row_id"):
        op.create_index(
            "idx_payment_collections_bank_row_id",
            "payment_collections",
            ["bank_row_id"],
            unique=False,
        )


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        _upgrade_postgresql()
    else:
        _upgrade_generic()


def downgrade() -> None:
    # Forward-only migration. Keep source tracking metadata once introduced.
    return None
