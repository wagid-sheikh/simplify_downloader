import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT_PARENT = ROOT.parent

for path in (ROOT, PROJECT_PARENT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from app.crypto import encrypt_secret


def _default_system_config_rows(secret_key: str) -> dict[str, str]:
    rows = {
        "TD_GLOBAL_USERNAME": "reports@example.com",
        "TD_STORAGE_STATE_FILENAME": "storage_state.json",
        "TD_BASE_URL": "https://simplifytumbledry.in",
        "TD_HOME_URL": "https://simplifytumbledry.in/home",
        "TD_LOGIN_URL": "https://simplifytumbledry.in/home/login",
        "TMS_BASE": "https://simplifytumbledry.in",
        "TD_STORE_DASHBOARD_PATH": "/mis/partner_dashboard?store_code={store_code}",
        "INGEST_BATCH_SIZE": "3000",
        "REPORT_EMAIL_FROM": "reports@example.com",
        "REPORT_EMAIL_SMTP_HOST": "smtp.example.com",
        "REPORT_EMAIL_SMTP_PORT": "587",
        "REPORT_EMAIL_SMTP_USERNAME": "reports@example.com",
        "REPORT_EMAIL_USE_TLS": "true",
        "PDF_RENDER_BACKEND": "bundled_chromium",
        "PDF_RENDER_HEADLESS": "true",
        "ETL_HEADLESS": "true",
        "pipeline_skip_dom_logging": "false",
        "skip_lead_assignment": "false",
        "ETL_STEP_TIMEOUT_SECONDS": "30",
        "PDF_RENDER_TIMEOUT_SECONDS": "30",
        "SKIP_UC_Pending_Delivery": "false",
    }
    rows["TD_GLOBAL_PASSWORD"] = encrypt_secret(secret_key, "change-me-global-password")
    rows["REPORT_EMAIL_SMTP_PASSWORD"] = encrypt_secret(secret_key, "change-me-smtp-password")
    return rows


def _initialize_system_config(db_path: Path, secret_key: str) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS system_config")
    conn.execute(
        """
        CREATE TABLE system_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE,
            value TEXT NOT NULL,
            description TEXT,
            is_active BOOLEAN NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    rows = _default_system_config_rows(secret_key)
    for key, value in rows.items():
        conn.execute(
            "INSERT INTO system_config (key, value, description, is_active) VALUES (?, ?, ?, 1)",
            (key, value, f"seed value for {key}",),
        )
    conn.commit()
    conn.close()


TEST_SECRET = "test-secret"
TEST_DB_PATH = ROOT / "tests" / "config.db"
TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_initialize_system_config(TEST_DB_PATH, TEST_SECRET)

reports_root = ROOT / "tests" / "reports"
reports_root.mkdir(parents=True, exist_ok=True)
log_file_path = ROOT / "tests" / "logs" / "app.jsonl"
log_file_path.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_ENV = {
    "SECRET_KEY": TEST_SECRET,
    "RUN_ENV": "test",
    "ENVIRONMENT": "test",
    "PIPELINE_TIMEZONE": "Asia/Kolkata",
    "ALEMBIC_CONFIG": "alembic.ini",
    "REPORTS_ROOT": str(reports_root),
    "JSON_LOG_FILE": str(log_file_path),
    "PDF_RENDER_CHROME_EXECUTABLE": "/usr/bin/google-chrome",
    "POSTGRES_HOST": "sqlite",
    "POSTGRES_PORT": "0",
    "POSTGRES_DB": str(TEST_DB_PATH),
    "POSTGRES_USER": "unused",
    "POSTGRES_PASSWORD": "unused",
}

for key, value in DEFAULT_ENV.items():
    os.environ.setdefault(key, value)
