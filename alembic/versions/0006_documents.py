"""Create documents table."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0006_documents"
down_revision = "0005_pipeline_run_summaries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("doc_type", sa.String(length=50), nullable=False),
        sa.Column("doc_subtype", sa.String(length=50), nullable=True),
        sa.Column("doc_date", sa.Date(), nullable=True),
        sa.Column("reference_name_1", sa.String(length=50), nullable=True),
        sa.Column("reference_id_1", sa.String(length=100), nullable=True),
        sa.Column("reference_name_2", sa.String(length=50), nullable=True),
        sa.Column("reference_id_2", sa.String(length=100), nullable=True),
        sa.Column("reference_name_3", sa.String(length=50), nullable=True),
        sa.Column("reference_id_3", sa.String(length=100), nullable=True),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("mime_type", sa.String(length=100), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("storage_backend", sa.String(length=10), nullable=False, server_default="fs"),
        sa.CheckConstraint("storage_backend IN ('fs','db','s3')", name="ck_documents_storage_backend"),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("file_blob", sa.LargeBinary(), nullable=True),
        sa.Column("checksum", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ok"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_by", sa.String(length=64), nullable=True),
    )

    op.create_index(
        "idx_documents_ref1",
        "documents",
        ["reference_name_1", "reference_id_1"],
    )
    op.create_index(
        "idx_documents_ref2",
        "documents",
        ["reference_name_2", "reference_id_2"],
    )
    op.create_index(
        "idx_documents_ref3",
        "documents",
        ["reference_name_3", "reference_id_3"],
    )
    op.create_index(
        "idx_documents_type",
        "documents",
        ["doc_type", "doc_subtype", "doc_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_documents_type", table_name="documents")
    op.drop_index("idx_documents_ref3", table_name="documents")
    op.drop_index("idx_documents_ref2", table_name="documents")
    op.drop_index("idx_documents_ref1", table_name="documents")
    op.drop_table("documents")
