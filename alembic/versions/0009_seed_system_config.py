"""Seed system_config with required values"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from simplify_downloader.crypto import encrypt_secret


revision = "0009_seed_system_config"
down_revision = "0008_system_config_table"
branch_labels = None
depends_on = None


PLAINTEXT_VALUES = [
    ("TD_GLOBAL_USERNAME", "A668", "Global SimpliFy dashboard username"),
    ("TD_STORAGE_STATE_FILENAME", "storage_state.json", "Default Playwright storage state"),
    ("TD_BASE_URL", "https://simplifytumbledry.in", "Base URL for Simplify dashboard"),
    ("TD_HOME_URL", "https://simplifytumbledry.in/home", "Dashboard home URL"),
    ("TD_LOGIN_URL", "https://simplifytumbledry.in/home/login", "Dashboard login URL"),
    ("TMS_BASE", "https://tms.simplifytumbledry.in", "Base URL for TMS portal"),
    (
        "TD_STORE_DASHBOARD_PATH",
        "/mis/partner_dashboard?store_code={store_code}",
        "Dashboard path template for stores",
    ),
    ("STORES_LIST", "A668,A817,A526", "Default scraping store codes"),
    ("REPORT_STORES_LIST", "A668,A817", "Store codes for reporting pipelines"),
    ("INGEST_BATCH_SIZE", "3000", "Batch size for CSV ingestion"),
    ("REPORT_EMAIL_FROM", "shaw@theshawventures.com", "Default From address for report emails"),
    ("REPORT_EMAIL_SMTP_HOST", "smtp.gmail.com", "SMTP host for report notifications"),
    ("REPORT_EMAIL_SMTP_PORT", "587", "SMTP port for report notifications"),
    (
        "REPORT_EMAIL_SMTP_USERNAME",
        "wagid.sheikh@gmail.com",
        "SMTP username for report notifications",
    ),
    ("REPORT_EMAIL_USE_TLS", "true", "Whether to enable TLS for SMTP"),
    ("PDF_RENDER_BACKEND", "bundled_chromium", "PDF rendering backend"),
    ("PDF_RENDER_HEADLESS", "true", "Whether headless rendering is enabled"),
]

ENCRYPTED_VALUES = [
    ("TD_GLOBAL_PASSWORD", "some-random-password", "Global dashboard password"),
    (
        "REPORT_EMAIL_SMTP_PASSWORD",
        "some-random-smtp-password",
        "SMTP password for report notifications",
    ),
]


def _require_secret_key() -> str:
    secret_key = os.getenv("SECRET_KEY")
    if not secret_key:
        raise RuntimeError("SECRET_KEY must be set to seed system_config")
    return secret_key


def _upsert(connection, *, key: str, value: str, description: str) -> None:
    existing = connection.execute(
        sa.text("SELECT id FROM system_config WHERE key = :key"), {"key": key}
    ).scalar()
    params = {"key": key, "value": value, "description": description}
    if existing:
        connection.execute(
            sa.text(
                """
                UPDATE system_config
                SET value = :value,
                    description = :description,
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE key = :key
                """
            ),
            params,
        )
    else:
        connection.execute(
            sa.text(
                """
                INSERT INTO system_config (key, value, description, is_active)
                VALUES (:key, :value, :description, TRUE)
                """
            ),
            params,
        )


def upgrade() -> None:
    connection = op.get_bind()
    secret_key = _require_secret_key()

    for key, value, description in PLAINTEXT_VALUES:
        _upsert(connection, key=key, value=value, description=description)

    for key, plaintext, description in ENCRYPTED_VALUES:
        ciphertext = encrypt_secret(secret_key, plaintext)
        _upsert(connection, key=key, value=ciphertext, description=description)


def downgrade() -> None:
    connection = op.get_bind()
    for key, _, _ in PLAINTEXT_VALUES + ENCRYPTED_VALUES:
        connection.execute(sa.text("DELETE FROM system_config WHERE key = :key"), {"key": key})
