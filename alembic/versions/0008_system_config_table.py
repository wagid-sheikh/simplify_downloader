"""Create or align system_config table"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0008_system_config_table"
down_revision = "0007_notification_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("system_config"):
        op.create_table(
            "system_config",
            sa.Column("id", sa.BigInteger(), primary_key=True),
            sa.Column("key", sa.Text(), nullable=False),
            sa.Column("value", sa.Text(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.UniqueConstraint("key", name="uq_system_config_key"),
        )
        return

    existing_columns = {column["name"] for column in inspector.get_columns("system_config")}

    if "description" not in existing_columns:
        op.add_column("system_config", sa.Column("description", sa.Text(), nullable=True))

    if "is_active" not in existing_columns:
        op.add_column(
            "system_config",
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )
    else:
        op.alter_column(
            "system_config",
            "is_active",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        )

    timestamp_columns = ("created_at", "updated_at")
    for column_name in timestamp_columns:
        if column_name not in existing_columns:
            op.add_column(
                "system_config",
                sa.Column(
                    column_name,
                    sa.DateTime(timezone=True),
                    nullable=False,
                    server_default=sa.text("now()"),
                ),
            )
        else:
            op.alter_column(
                "system_config",
                column_name,
                existing_type=sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            )

    for column_name in ("key", "value"):
        if column_name in existing_columns:
            op.alter_column(
                "system_config",
                column_name,
                existing_type=sa.Text(),
                nullable=False,
            )

    unique_constraints = {constraint["name"] for constraint in inspector.get_unique_constraints("system_config")}
    if "uq_system_config_key" not in unique_constraints:
        op.create_unique_constraint("uq_system_config_key", "system_config", ["key"])


def downgrade() -> None:
    op.drop_table("system_config")
