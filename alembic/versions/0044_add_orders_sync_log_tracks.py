"""Add primary/secondary metrics to orders_sync_log."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0044_add_orders_sync_log_tracks"
down_revision = "0043_leadsassignmentsummarytempl"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("orders_sync_log", sa.Column("primary_rows_downloaded", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_rows_ingested", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_staging_rows", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_staging_inserted", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_staging_updated", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_final_inserted", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("primary_final_updated", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_rows_downloaded", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_rows_ingested", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_staging_rows", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_staging_inserted", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_staging_updated", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_final_inserted", sa.BigInteger()))
    op.add_column("orders_sync_log", sa.Column("secondary_final_updated", sa.BigInteger()))

    op.execute(
        """
        UPDATE orders_sync_log
        SET
            primary_rows_downloaded = COALESCE(primary_rows_downloaded, 0),
            primary_rows_ingested = COALESCE(primary_rows_ingested, 0),
            primary_staging_rows = COALESCE(primary_staging_rows, 0),
            primary_staging_inserted = COALESCE(primary_staging_inserted, 0),
            primary_staging_updated = COALESCE(primary_staging_updated, 0),
            primary_final_inserted = COALESCE(primary_final_inserted, 0),
            primary_final_updated = COALESCE(primary_final_updated, 0),
            secondary_rows_downloaded = COALESCE(secondary_rows_downloaded, 0),
            secondary_rows_ingested = COALESCE(secondary_rows_ingested, 0),
            secondary_staging_rows = COALESCE(secondary_staging_rows, 0),
            secondary_staging_inserted = COALESCE(secondary_staging_inserted, 0),
            secondary_staging_updated = COALESCE(secondary_staging_updated, 0),
            secondary_final_inserted = COALESCE(secondary_final_inserted, 0),
            secondary_final_updated = COALESCE(secondary_final_updated, 0)
        """
    )


def downgrade() -> None:
    op.drop_column("orders_sync_log", "secondary_final_updated")
    op.drop_column("orders_sync_log", "secondary_final_inserted")
    op.drop_column("orders_sync_log", "secondary_staging_updated")
    op.drop_column("orders_sync_log", "secondary_staging_inserted")
    op.drop_column("orders_sync_log", "secondary_staging_rows")
    op.drop_column("orders_sync_log", "secondary_rows_ingested")
    op.drop_column("orders_sync_log", "secondary_rows_downloaded")
    op.drop_column("orders_sync_log", "primary_final_updated")
    op.drop_column("orders_sync_log", "primary_final_inserted")
    op.drop_column("orders_sync_log", "primary_staging_updated")
    op.drop_column("orders_sync_log", "primary_staging_inserted")
    op.drop_column("orders_sync_log", "primary_staging_rows")
    op.drop_column("orders_sync_log", "primary_rows_ingested")
    op.drop_column("orders_sync_log", "primary_rows_downloaded")
