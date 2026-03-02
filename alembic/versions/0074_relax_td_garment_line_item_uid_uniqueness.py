"""Relax TD garment line-item UID uniqueness and add ingest row sequence."""

from alembic import op
import sqlalchemy as sa


revision = "0074_relax_td_garment_line_item_uid_uniqueness"
down_revision = "0073_add_td_compare_threshold"
branch_labels = None
depends_on = None


STG_UQ = "uq_stg_td_garments_store_line_item_uid"
LINE_ITEM_UQ = "uq_order_line_items_cost_center_line_item_uid"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        op.drop_index(STG_UQ, table_name="stg_td_garments")
        op.drop_index(LINE_ITEM_UQ, table_name="order_line_items")
        op.add_column("stg_td_garments", sa.Column("ingest_row_seq", sa.Integer(), nullable=True))
        op.add_column("order_line_items", sa.Column("ingest_row_seq", sa.Integer(), nullable=True))
    else:
        with op.batch_alter_table("stg_td_garments") as batch_op:
            batch_op.drop_constraint(STG_UQ, type_="unique")
            batch_op.add_column(sa.Column("ingest_row_seq", sa.Integer(), nullable=True))

        with op.batch_alter_table("order_line_items") as batch_op:
            batch_op.drop_constraint(LINE_ITEM_UQ, type_="unique")
            batch_op.add_column(sa.Column("ingest_row_seq", sa.Integer(), nullable=True))

    op.execute("UPDATE stg_td_garments SET ingest_row_seq = id WHERE ingest_row_seq IS NULL")
    op.execute("UPDATE order_line_items SET ingest_row_seq = id WHERE ingest_row_seq IS NULL")

    with op.batch_alter_table("stg_td_garments") as batch_op:
        batch_op.alter_column("ingest_row_seq", existing_type=sa.Integer(), nullable=False)

    with op.batch_alter_table("order_line_items") as batch_op:
        batch_op.alter_column("ingest_row_seq", existing_type=sa.Integer(), nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("order_line_items") as batch_op:
        batch_op.drop_column("ingest_row_seq")

    with op.batch_alter_table("stg_td_garments") as batch_op:
        batch_op.drop_column("ingest_row_seq")

    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        op.create_index(LINE_ITEM_UQ, "order_line_items", ["cost_center", "line_item_uid"], unique=True)
        op.create_index(STG_UQ, "stg_td_garments", ["store_code", "line_item_uid"], unique=True)
    else:
        op.create_unique_constraint(
            LINE_ITEM_UQ,
            "order_line_items",
            ["cost_center", "line_item_uid"],
        )
        op.create_unique_constraint(
            STG_UQ,
            "stg_td_garments",
            ["store_code", "line_item_uid"],
        )
