import os
import sqlite3
from pathlib import Path

import pytest

from app.config import Config, ConfigError
from app.crypto import encrypt_secret


REQUIRED_ENV_KEYS = [
    "SECRET_KEY",
    "RUN_ENV",
    "ENVIRONMENT",
    "PIPELINE_TIMEZONE",
    "ALEMBIC_CONFIG",
    "REPORTS_ROOT",
    "JSON_LOG_FILE",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]


def _base_rows(secret_key: str) -> dict[str, str]:
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
    }
    rows["TD_GLOBAL_PASSWORD"] = encrypt_secret(secret_key, "change-me-global-password")
    rows["REPORT_EMAIL_SMTP_PASSWORD"] = encrypt_secret(secret_key, "change-me-smtp-password")
    return rows


def _write_system_config(db_path: Path, rows: dict[str, str]) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
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
    for key, value in rows.items():
        conn.execute(
            "INSERT INTO system_config (key, value, description, is_active) VALUES (?, ?, ?, 1)",
            (key, value, f"test value for {key}"),
        )
    conn.commit()
    conn.close()


def _set_env(monkeypatch: pytest.MonkeyPatch, overrides: dict[str, str]) -> None:
    defaults = {
        "SECRET_KEY": "unit-test-secret",
        "RUN_ENV": "test",
        "ENVIRONMENT": "test",
        "PIPELINE_TIMEZONE": "Asia/Kolkata",
        "ALEMBIC_CONFIG": "alembic.ini",
        "POSTGRES_HOST": "sqlite",
        "POSTGRES_PORT": "0",
        "POSTGRES_DB": "",
        "POSTGRES_USER": "unused",
        "POSTGRES_PASSWORD": "unused",
    }
    defaults.update(overrides)
    monkeypatch.delenv("PDF_RENDER_CHROME_EXECUTABLE", raising=False)
    missing = [key for key in REQUIRED_ENV_KEYS if key not in defaults]
    if missing:
        raise AssertionError(f"Missing env defaults for: {missing}")
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)


def test_config_loads_expected_values(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    log_file = tmp_path / "logs" / "test.jsonl"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(log_file),
        },
    )

    cfg = Config.load_from_env_and_db()

    assert cfg.td_global_username == "reports@example.com"
    assert cfg.td_global_password == "change-me-global-password"
    assert cfg.report_email_smtp_port == 587
    assert cfg.report_email_use_tls is True
    assert cfg.pdf_render_headless is True
    assert cfg.report_email_smtp_password == "change-me-smtp-password"
    assert cfg.pdf_render_chrome_executable is None
    assert cfg.pipeline_skip_dom_logging is False


def test_missing_env_variable_raises(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    _write_system_config(db_path, _base_rows("unit-test-secret"))
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    log_file = tmp_path / "logs.jsonl"
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(log_file),
        },
    )
    monkeypatch.delenv("SECRET_KEY")

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_missing_system_config_key(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows.pop("TD_BASE_URL")
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
        },
    )

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_invalid_integer_value(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows["INGEST_BATCH_SIZE"] = "abc"
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
        },
    )

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_invalid_boolean_value(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows["REPORT_EMAIL_USE_TLS"] = "maybe"
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
        },
    )

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_local_chrome_requires_executable(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows["PDF_RENDER_BACKEND"] = "local_chrome"
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
        },
    )

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_local_chrome_accepts_executable(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows["PDF_RENDER_BACKEND"] = "local_chrome"
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
            "PDF_RENDER_CHROME_EXECUTABLE": "/usr/bin/google-chrome",
        },
    )

    cfg = Config.load_from_env_and_db()

    assert cfg.pdf_render_chrome_executable == "/usr/bin/google-chrome"


def test_encrypted_value_must_be_valid(monkeypatch, tmp_path):
    db_path = tmp_path / "config.sqlite"
    rows = _base_rows("unit-test-secret")
    rows["TD_GLOBAL_PASSWORD"] = "invalid-token"
    _write_system_config(db_path, rows)
    reports_root = tmp_path / "reports"
    reports_root.mkdir()
    _set_env(
        monkeypatch,
        {
            "POSTGRES_DB": str(db_path),
            "REPORTS_ROOT": str(reports_root),
            "JSON_LOG_FILE": str(tmp_path / "logs.jsonl"),
        },
    )

    with pytest.raises(ConfigError):
        Config.load_from_env_and_db()


def test_os_getenv_usage_restricted():
    repo_root = Path(__file__).resolve().parents[1]
    allowed = {
        repo_root / "config.py",
        repo_root / "crypto.py",
        repo_root / "alembic" / "env.py",
        repo_root
        / "alembic"
        / "versions"
        / "0009_seed_system_config.py",
        repo_root / "app" / "config.py",
    }
    offenders: list[Path] = []
    for path in repo_root.rglob("*.py"):
        if path in allowed or "tests" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "os.getenv" in text or "os.environ" in text:
            offenders.append(path)
    assert offenders == []
