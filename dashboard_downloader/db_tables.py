from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

metadata = sa.MetaData()


pipeline_run_summaries = sa.Table(
    "pipeline_run_summaries",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("pipeline_name", sa.String(length=100)),
    sa.Column("run_id", sa.String(length=64)),
    sa.Column("run_env", sa.String(length=32)),
    sa.Column("started_at", sa.DateTime(timezone=True)),
    sa.Column("finished_at", sa.DateTime(timezone=True)),
    sa.Column("total_time_taken", sa.String(length=8)),
    sa.Column("report_date", sa.Date()),
    sa.Column("overall_status", sa.String(length=32)),
    sa.Column("summary_text", sa.Text()),
    sa.Column("phases_json", postgresql.JSONB(astext_type=sa.Text())),
    sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text())),
    sa.Column("created_at", sa.DateTime(timezone=True)),
)


documents = sa.Table(
    "documents",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("doc_type", sa.String(length=50)),
    sa.Column("doc_subtype", sa.String(length=50)),
    sa.Column("doc_date", sa.Date()),
    sa.Column("reference_name_1", sa.String(length=50)),
    sa.Column("reference_id_1", sa.String(length=100)),
    sa.Column("reference_name_2", sa.String(length=50)),
    sa.Column("reference_id_2", sa.String(length=100)),
    sa.Column("reference_name_3", sa.String(length=50)),
    sa.Column("reference_id_3", sa.String(length=100)),
    sa.Column("file_name", sa.Text()),
    sa.Column("mime_type", sa.String(length=100)),
    sa.Column("file_size_bytes", sa.BigInteger()),
    sa.Column("storage_backend", sa.String(length=10)),
    sa.Column("file_path", sa.Text()),
    sa.Column("file_blob", sa.LargeBinary()),
    sa.Column("checksum", sa.Text()),
    sa.Column("status", sa.String(length=32)),
    sa.Column("error_message", sa.Text()),
    sa.Column("created_at", sa.DateTime(timezone=True)),
    sa.Column("created_by", sa.String(length=64)),
)


pipelines = sa.Table(
    "pipelines",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("code", sa.Text()),
    sa.Column("description", sa.Text()),
)


notification_profiles = sa.Table(
    "notification_profiles",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("pipeline_id", sa.BigInteger()),
    sa.Column("code", sa.Text()),
    sa.Column("description", sa.Text()),
    sa.Column("env", sa.Text()),
    sa.Column("scope", sa.Text()),
    sa.Column("attach_mode", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
)


email_templates = sa.Table(
    "email_templates",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("profile_id", sa.BigInteger()),
    sa.Column("name", sa.Text()),
    sa.Column("subject_template", sa.Text()),
    sa.Column("body_template", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
)


notification_recipients = sa.Table(
    "notification_recipients",
    metadata,
    sa.Column("id", sa.BigInteger()),
    sa.Column("profile_id", sa.BigInteger()),
    sa.Column("store_code", sa.Text()),
    sa.Column("env", sa.Text()),
    sa.Column("email_address", sa.Text()),
    sa.Column("display_name", sa.Text()),
    sa.Column("send_as", sa.Text()),
    sa.Column("is_active", sa.Boolean()),
    sa.Column("created_at", sa.DateTime(timezone=True)),
)
