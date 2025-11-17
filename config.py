"""
CONFIG.PY — SINGLE SOURCE OF TRUTH (SSOT)

This module is the ONLY place allowed to:
- Read environment variables
- Query the system_config table
- Decrypt DB-stored encrypted values

ALL REQUIRED VARIABLES MUST EXIST — NO DEFAULTS.
If any variable (env or DB) is missing or invalid, the system MUST fail early.

All config is loaded ONCE at startup and cached in a single in-memory Config object.
No dynamic reload. No direct DB or env reads outside this module.

TD_GLOBAL_PASSWORD and REPORT_EMAIL_SMTP_PASSWORD are stored encrypted in DB
and must be decrypted here using SECRET_KEY from environment.

All new settings MUST go into system_config unless explicitly marked env-only.
To use a config value, import:

    from simplify_downloader.config import config

Do not access os.getenv or system_config directly from any other module.
"""

from __future__ import annotations

import asyncio
import binascii
import logging
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, TypeVar

from dotenv import load_dotenv

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from simplify_downloader.crypto import decrypt_secret


# Determine project root correctly (directory containing the top-level package)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load variables from .env if it exists; OS env overrides these automatically
load_dotenv(PROJECT_ROOT / ".env")

if os.getenv("DEBUG_CONFIG") == "1":
    print("[CONFIG] Loaded .env from:", PROJECT_ROOT / ".env")


logger = logging.getLogger(__name__)

ENV_ONLY_KEYS = [
    "SECRET_KEY",
    "RUN_ENV",
    "ENVIRONMENT",
    "PIPELINE_TIMEZONE",
    "DATABASE_URL",
    "ALEMBIC_CONFIG",
    "REPORTS_ROOT",
    "JSON_LOG_FILE",
    "PDF_RENDER_CHROME_EXECUTABLE",
]

PLAINTEXT_DB_KEYS = [
    "TD_GLOBAL_USERNAME",
    "TD_STORAGE_STATE_FILENAME",
    "TD_BASE_URL",
    "TD_HOME_URL",
    "TD_LOGIN_URL",
    "TMS_BASE",
    "TD_STORE_DASHBOARD_PATH",
    "STORES_LIST",
    "REPORT_STORES_LIST",
    "INGEST_BATCH_SIZE",
    "REPORT_EMAIL_FROM",
    "REPORT_EMAIL_SMTP_HOST",
    "REPORT_EMAIL_SMTP_PORT",
    "REPORT_EMAIL_SMTP_USERNAME",
    "REPORT_EMAIL_USE_TLS",
    "PDF_RENDER_BACKEND",
    "PDF_RENDER_HEADLESS",
]

ENCRYPTED_DB_KEYS = [
    "TD_GLOBAL_PASSWORD",
    "REPORT_EMAIL_SMTP_PASSWORD",
]

REQUIRED_DB_KEYS = PLAINTEXT_DB_KEYS + ENCRYPTED_DB_KEYS

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded."""


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        message = f"Missing required environment variable: {key}"
        logger.error(message)
        raise ConfigError(message)
    stripped = value.strip()
    if not stripped:
        message = f"Environment variable {key} cannot be blank"
        logger.error(message)
        raise ConfigError(message)
    return stripped


def _load_env_values() -> Dict[str, str]:
    return {key: _require_env(key) for key in ENV_ONLY_KEYS}


def _parse_bool(value: str, *, key: str) -> bool:
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    message = f"Config key {key} must be a boolean string; got {value!r}"
    logger.error(message)
    raise ConfigError(message)


def _parse_int(value: str, *, key: str) -> int:
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        message = f"Config key {key} must be an integer; got {value!r}"
        logger.error(message)
        raise ConfigError(message)


def _parse_list(value: str) -> list[str]:
    if not value:
        return []
    tokens = re.split(r"[,\n]", value)
    return [token.strip() for token in tokens if token and token.strip()]


def _clean_url(value: str, *, key: str) -> str:
    stripped = value.strip().rstrip("/")
    if not stripped:
        message = f"Config key {key} cannot be blank"
        logger.error(message)
        raise ConfigError(message)
    return stripped


def _clean_text(value: str, *, key: str) -> str:
    stripped = value.strip()
    if not stripped:
        message = f"Config key {key} cannot be blank"
        logger.error(message)
        raise ConfigError(message)
    return stripped


T = TypeVar("T")


def _run_async_blocking(task_factory: Callable[[], Awaitable[T]]) -> T:
    result: dict[str, Any] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(task_factory())
        except BaseException as exc:  # pragma: no cover - re-raised in caller
            result["error"] = exc

    thread = threading.Thread(target=_runner, name="config-db-loader", daemon=True)
    thread.start()
    thread.join()

    if "error" in result:
        raise result["error"]
    return result["value"]


async def _fetch_system_config_async(database_url: str) -> Dict[str, str]:
    engine: AsyncEngine | None = None
    try:
        engine = create_async_engine(database_url, future=True)
        async with engine.connect() as connection:
            rows = await connection.execute(
                text("SELECT key, value FROM system_config WHERE is_active = TRUE")
            )
            return {row.key: row.value for row in rows}
    except SQLAlchemyError as exc:
        message = "Unable to load configuration from system_config"
        logger.exception(message)
        raise ConfigError(message) from exc
    finally:
        if engine is not None:
            await engine.dispose()


def _load_system_config(database_url: str) -> Dict[str, str]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_fetch_system_config_async(database_url))
    else:
        return _run_async_blocking(lambda: _fetch_system_config_async(database_url))


def _decrypt_db_values(secret_key: str, db_values: Mapping[str, str]) -> Dict[str, str]:
    decrypted: Dict[str, str] = {}
    for key in ENCRYPTED_DB_KEYS:
        ciphertext = db_values.get(key)
        if ciphertext is None:
            message = f"Missing encrypted system_config key: {key}"
            logger.error(message)
            raise ConfigError(message)
        try:
            decrypted[key] = decrypt_secret(secret_key, ciphertext)
        except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
            message = f"Failed to decrypt system_config key: {key}"
            logger.exception(message)
            raise ConfigError(message) from exc
    return decrypted


@dataclass(slots=True, frozen=True)
class Config:
    run_env: str
    environment: str
    pipeline_timezone: str
    database_url: str
    alembic_config: str
    reports_root: str
    json_log_file: str
    pdf_render_chrome_executable: str

    td_storage_state_filename: str
    td_base_url: str
    td_home_url: str
    td_login_url: str
    tms_base: str
    td_store_dashboard_path: str
    td_global_username: str
    td_global_password: str
    stores_list: list[str]
    report_stores_list: list[str]
    ingest_batch_size: int
    report_email_from: str
    report_email_smtp_host: str
    report_email_smtp_port: int
    report_email_smtp_username: str
    report_email_smtp_password: str
    report_email_use_tls: bool
    pdf_render_backend: str
    pdf_render_headless: bool

    @classmethod
    def load_from_env_and_db(cls) -> Config:
        env_values = _load_env_values()
        secret_key = env_values["SECRET_KEY"]
        database_url = env_values["DATABASE_URL"]
        db_values = _load_system_config(database_url)

        missing = [key for key in REQUIRED_DB_KEYS if key not in db_values]
        if missing:
            message = f"Missing required system_config keys: {', '.join(sorted(missing))}"
            logger.error(message)
            raise ConfigError(message)

        decrypted_values = _decrypt_db_values(secret_key, db_values)

        td_base_url = _clean_url(db_values["TD_BASE_URL"], key="TD_BASE_URL")
        tms_base = _clean_url(db_values["TMS_BASE"], key="TMS_BASE")
        td_login_url = _clean_url(db_values["TD_LOGIN_URL"], key="TD_LOGIN_URL")
        td_home_url = _clean_url(db_values["TD_HOME_URL"], key="TD_HOME_URL")

        stores_list = _parse_list(db_values["STORES_LIST"])
        report_stores_list = _parse_list(db_values["REPORT_STORES_LIST"])

        ingest_batch_size = _parse_int(db_values["INGEST_BATCH_SIZE"], key="INGEST_BATCH_SIZE")
        report_email_smtp_port = _parse_int(
            db_values["REPORT_EMAIL_SMTP_PORT"], key="REPORT_EMAIL_SMTP_PORT"
        )

        report_email_use_tls = _parse_bool(
            db_values["REPORT_EMAIL_USE_TLS"], key="REPORT_EMAIL_USE_TLS"
        )
        pdf_render_headless = _parse_bool(
            db_values["PDF_RENDER_HEADLESS"], key="PDF_RENDER_HEADLESS"
        )

        td_store_dashboard_path = _clean_text(
            db_values["TD_STORE_DASHBOARD_PATH"], key="TD_STORE_DASHBOARD_PATH"
        )
        td_storage_state_filename = _clean_text(
            db_values["TD_STORAGE_STATE_FILENAME"], key="TD_STORAGE_STATE_FILENAME"
        )

        pdf_render_backend = _clean_text(
            db_values["PDF_RENDER_BACKEND"], key="PDF_RENDER_BACKEND"
        )

        report_email_from = _clean_text(db_values["REPORT_EMAIL_FROM"], key="REPORT_EMAIL_FROM")
        report_email_smtp_host = _clean_text(
            db_values["REPORT_EMAIL_SMTP_HOST"], key="REPORT_EMAIL_SMTP_HOST"
        )
        report_email_smtp_username = _clean_text(
            db_values["REPORT_EMAIL_SMTP_USERNAME"], key="REPORT_EMAIL_SMTP_USERNAME"
        )

        td_global_username = _clean_text(
            db_values["TD_GLOBAL_USERNAME"], key="TD_GLOBAL_USERNAME"
        )
        td_global_password = _clean_text(
            decrypted_values["TD_GLOBAL_PASSWORD"], key="TD_GLOBAL_PASSWORD"
        )
        report_email_smtp_password = _clean_text(
            decrypted_values["REPORT_EMAIL_SMTP_PASSWORD"], key="REPORT_EMAIL_SMTP_PASSWORD"
        )

        reports_root = env_values["REPORTS_ROOT"]
        json_log_file = env_values["JSON_LOG_FILE"]
        chrome_exec = env_values["PDF_RENDER_CHROME_EXECUTABLE"]

        return cls(
            run_env=env_values["RUN_ENV"],
            environment=env_values["ENVIRONMENT"],
            pipeline_timezone=env_values["PIPELINE_TIMEZONE"],
            database_url=database_url,
            alembic_config=env_values["ALEMBIC_CONFIG"],
            reports_root=reports_root,
            json_log_file=json_log_file,
            pdf_render_chrome_executable=chrome_exec,
            td_storage_state_filename=td_storage_state_filename,
            td_base_url=td_base_url,
            td_home_url=td_home_url,
            td_login_url=td_login_url,
            tms_base=tms_base,
            td_store_dashboard_path=td_store_dashboard_path,
            td_global_username=td_global_username,
            td_global_password=td_global_password,
            stores_list=stores_list,
            report_stores_list=report_stores_list,
            ingest_batch_size=ingest_batch_size,
            report_email_from=report_email_from,
            report_email_smtp_host=report_email_smtp_host,
            report_email_smtp_port=report_email_smtp_port,
            report_email_smtp_username=report_email_smtp_username,
            report_email_smtp_password=report_email_smtp_password,
            report_email_use_tls=report_email_use_tls,
            pdf_render_backend=pdf_render_backend,
            pdf_render_headless=pdf_render_headless,
        )


config = Config.load_from_env_and_db()
