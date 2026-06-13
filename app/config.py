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

    from app.config import config

Do not access os.getenv or system_config directly from any other module.
"""

from __future__ import annotations

import asyncio
import binascii
import logging
import os
import threading
import sys
from urllib.parse import quote_plus
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, TypeVar

from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.crypto import decrypt_secret


# Determine project root correctly (repository root lives one level above app/)
# so that we can locate the .env file and other top-level resources.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load variables from .env if it exists; OS env overrides these automatically
load_dotenv(PROJECT_ROOT / ".env")

if os.getenv("DEBUG_CONFIG") == "1":
    print("[CONFIG] Loaded .env from:", PROJECT_ROOT / ".env")


logger = logging.getLogger(__name__)

DEFAULT_TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS = 90
DEFAULT_TD_BROWSER_OPERATION_TIMEOUT_SECONDS = DEFAULT_TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS
DEFAULT_TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS = 10
DEFAULT_TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS = 240
DEFAULT_TD_LEADS_GATHER_TIMEOUT_SECONDS = 270
DEFAULT_TD_LEADS_CANCELLATION_DRAIN_TIMEOUT_SECONDS = 10
DEFAULT_CUSTOMER_FOLLOWUP_BACKLOG_WARNING_THRESHOLD = 20

ENV_ONLY_KEYS = [
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

POSTGRES_ENV_KEYS = [
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]

PLAINTEXT_DB_KEYS = [
    "TD_GLOBAL_USERNAME",
    "TD_STORAGE_STATE_FILENAME",
    "TD_BASE_URL",
    "TD_HOME_URL",
    "TD_LOGIN_URL",
    "TMS_BASE",
    "TD_STORE_DASHBOARD_PATH",
    "INGEST_BATCH_SIZE",
    "REPORT_EMAIL_FROM",
    "REPORT_EMAIL_SMTP_HOST",
    "REPORT_EMAIL_SMTP_PORT",
    "REPORT_EMAIL_SMTP_USERNAME",
    "REPORT_EMAIL_USE_TLS",
    "PDF_RENDER_BACKEND",
    "PDF_RENDER_HEADLESS",
    "ETL_HEADLESS",
    "ETL_STEP_TIMEOUT_SECONDS",
    "PDF_RENDER_TIMEOUT_SECONDS",
    "pipeline_skip_dom_logging",
    "skip_lead_assignment",
    "UC_IGNORE_HTTPS_ERRORS",
]

ENCRYPTED_DB_KEYS = [
    "TD_GLOBAL_PASSWORD",
    "REPORT_EMAIL_SMTP_PASSWORD",
]

REQUIRED_DB_KEYS = PLAINTEXT_DB_KEYS + ENCRYPTED_DB_KEYS

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
DEFAULT_REPORT_EMAIL_SEND_MAX_ATTEMPTS = 3
DEFAULT_REPORT_EMAIL_SEND_INITIAL_DELAY_SECONDS = 1.0
DEFAULT_REPORT_EMAIL_SEND_MAX_DELAY_SECONDS = 30.0
DEFAULT_REPORT_EMAIL_SEND_TRANSIENT_EXCEPTIONS = (
    "ConnectionResetError",
    "socket.gaierror",
    "ssl.SSLEOFError",
    "TimeoutError",
    "smtplib.SMTPServerDisconnected",
    "smtplib.SMTPConnectError",
)
_NON_INTERACTIVE_OVERRIDE_LOGGED = False


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


def _build_database_url(env_values: Mapping[str, str]) -> str:
    missing = [key for key in POSTGRES_ENV_KEYS if key not in env_values]
    if missing:
        message = f"Missing required environment variables: {', '.join(sorted(missing))}"
        logger.error(message)
        raise ConfigError(message)

    host = _clean_text(env_values["POSTGRES_HOST"], key="POSTGRES_HOST")
    database = _clean_text(env_values["POSTGRES_DB"], key="POSTGRES_DB")

    if host.lower() == "sqlite":
        return f"sqlite:///{database}"

    port = _parse_int(env_values["POSTGRES_PORT"], key="POSTGRES_PORT")
    user = _clean_text(env_values["POSTGRES_USER"], key="POSTGRES_USER")
    password = _clean_text(env_values["POSTGRES_PASSWORD"], key="POSTGRES_PASSWORD")
    user_encoded = quote_plus(user)
    password_encoded = quote_plus(password)

    return f"postgresql+asyncpg://{user_encoded}:{password_encoded}@{host}:{port}/{database}"


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


def _parse_positive_int(value: str, *, key: str) -> int:
    parsed = _parse_int(value, key=key)
    if parsed < 1:
        raise ConfigError(f"Config key {key} must be >= 1")
    return parsed


def _parse_float(value: str, *, key: str) -> float:
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        message = f"Config key {key} must be a number; got {value!r}"
        logger.error(message)
        raise ConfigError(message)


def _clean_csv(value: str, *, key: str) -> tuple[str, ...]:
    entries = tuple(part.strip() for part in value.split(",") if part.strip())
    if not entries:
        message = f"Config key {key} must contain at least one value"
        logger.error(message)
        raise ConfigError(message)
    return entries


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


def is_non_interactive() -> bool:
    if os.getenv("CRON_TZ"):
        return True
    if not os.getenv("SHELL"):
        return True
    if os.getenv("TERM") == "dumb":
        return True

    has_display = bool(os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY"))
    if os.getenv("SSH_TTY") and not has_display:
        return True
    if sys.platform == "darwin" and not has_display:
        return True

    return False



def _clean_optional_path_config(value: str | None, *, default: str) -> str:
    raw = str(value or "").strip()
    return raw or default

def _warn_non_interactive_override(
    original_etl_headless: bool, original_pdf_render_headless: bool
) -> None:
    global _NON_INTERACTIVE_OVERRIDE_LOGGED
    if _NON_INTERACTIVE_OVERRIDE_LOGGED:
        return
    _NON_INTERACTIVE_OVERRIDE_LOGGED = True
    logger.warning(
        "Non-interactive environment detected; overriding ETL_HEADLESS=%s and "
        "PDF_RENDER_HEADLESS=%s to True.",
        original_etl_headless,
        original_pdf_render_headless,
    )


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


def _fetch_system_config_sync(database_url: str) -> Dict[str, str]:
    engine = create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                text("SELECT key, value FROM system_config WHERE is_active = TRUE")
            )
            return {row.key: row.value for row in rows}
    except SQLAlchemyError as exc:
        message = "Unable to load configuration from system_config"
        logger.exception(message)
        raise ConfigError(message) from exc
    finally:
        engine.dispose()


def _load_system_config(database_url: str) -> Dict[str, str]:
    if database_url.startswith("sqlite"):
        return _fetch_system_config_sync(database_url)

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
    pdf_render_chrome_executable: str | None

    td_storage_state_filename: str
    td_base_url: str
    td_home_url: str
    td_login_url: str
    tms_base: str
    td_store_dashboard_path: str
    td_global_username: str
    td_global_password: str
    ingest_batch_size: int
    report_email_from: str
    report_email_smtp_host: str
    report_email_smtp_port: int
    report_email_smtp_username: str
    report_email_smtp_password: str
    report_email_use_tls: bool
    report_email_send_max_attempts: int
    report_email_send_initial_delay_seconds: float
    report_email_send_max_delay_seconds: float
    report_email_send_transient_exceptions: tuple[str, ...]
    pdf_render_backend: str
    pdf_render_headless: bool
    etl_headless: bool
    etl_step_timeout_seconds: int
    pdf_render_timeout_seconds: int
    pipeline_skip_dom_logging: bool
    skip_lead_assignment: bool
    uc_ignore_https_errors: bool
    td_browser_operation_timeout_seconds: int
    td_leads_browser_operation_timeout_seconds: int
    td_leads_browser_cleanup_timeout_seconds: int
    td_leads_store_worker_timeout_seconds: int
    td_leads_gather_timeout_seconds: int
    td_leads_cancellation_drain_timeout_seconds: int
    customer_followup_input_dir: str
    customer_followup_external_input_dir: str
    customer_followup_archive_dir: str
    customer_followup_output_dir: str
    customer_followup_backlog_warning_threshold: int

    @classmethod
    def load_from_env_and_db(cls) -> Config:
        env_values = _load_env_values()
        secret_key = env_values["SECRET_KEY"]
        database_url = _build_database_url(env_values)
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

        ingest_batch_size = _parse_int(db_values["INGEST_BATCH_SIZE"], key="INGEST_BATCH_SIZE")
        report_email_smtp_port = _parse_int(
            db_values["REPORT_EMAIL_SMTP_PORT"], key="REPORT_EMAIL_SMTP_PORT"
        )

        report_email_use_tls = _parse_bool(
            db_values["REPORT_EMAIL_USE_TLS"], key="REPORT_EMAIL_USE_TLS"
        )
        report_email_send_max_attempts = _parse_int(
            db_values.get(
                "REPORT_EMAIL_SEND_MAX_ATTEMPTS",
                str(DEFAULT_REPORT_EMAIL_SEND_MAX_ATTEMPTS),
            ),
            key="REPORT_EMAIL_SEND_MAX_ATTEMPTS",
        )
        if report_email_send_max_attempts < 1:
            raise ConfigError("Config key REPORT_EMAIL_SEND_MAX_ATTEMPTS must be >= 1")
        report_email_send_initial_delay_seconds = _parse_float(
            db_values.get(
                "REPORT_EMAIL_SEND_INITIAL_DELAY_SECONDS",
                str(DEFAULT_REPORT_EMAIL_SEND_INITIAL_DELAY_SECONDS),
            ),
            key="REPORT_EMAIL_SEND_INITIAL_DELAY_SECONDS",
        )
        if report_email_send_initial_delay_seconds < 0:
            raise ConfigError("Config key REPORT_EMAIL_SEND_INITIAL_DELAY_SECONDS must be >= 0")
        report_email_send_max_delay_seconds = _parse_float(
            db_values.get(
                "REPORT_EMAIL_SEND_MAX_DELAY_SECONDS",
                str(DEFAULT_REPORT_EMAIL_SEND_MAX_DELAY_SECONDS),
            ),
            key="REPORT_EMAIL_SEND_MAX_DELAY_SECONDS",
        )
        if report_email_send_max_delay_seconds < 0:
            raise ConfigError("Config key REPORT_EMAIL_SEND_MAX_DELAY_SECONDS must be >= 0")
        report_email_send_transient_exceptions = _clean_csv(
            db_values.get(
                "REPORT_EMAIL_SEND_TRANSIENT_EXCEPTIONS",
                ",".join(DEFAULT_REPORT_EMAIL_SEND_TRANSIENT_EXCEPTIONS),
            ),
            key="REPORT_EMAIL_SEND_TRANSIENT_EXCEPTIONS",
        )
        pdf_render_headless = _parse_bool(
            db_values["PDF_RENDER_HEADLESS"], key="PDF_RENDER_HEADLESS"
        )
        etl_headless = _parse_bool(db_values["ETL_HEADLESS"], key="ETL_HEADLESS")
        if is_non_interactive():
            if not (etl_headless and pdf_render_headless):
                _warn_non_interactive_override(etl_headless, pdf_render_headless)
            etl_headless = True
            pdf_render_headless = True
        etl_step_timeout_seconds = _parse_int(
            db_values["ETL_STEP_TIMEOUT_SECONDS"], key="ETL_STEP_TIMEOUT_SECONDS"
        )
        pdf_render_timeout_seconds = _parse_int(
            db_values["PDF_RENDER_TIMEOUT_SECONDS"], key="PDF_RENDER_TIMEOUT_SECONDS"
        )
        pipeline_skip_dom_logging = _parse_bool(
            db_values["pipeline_skip_dom_logging"], key="pipeline_skip_dom_logging"
        )
        skip_lead_assignment = _parse_bool(
            db_values["skip_lead_assignment"], key="skip_lead_assignment"
        )
        uc_ignore_https_errors = _parse_bool(
            db_values["UC_IGNORE_HTTPS_ERRORS"], key="UC_IGNORE_HTTPS_ERRORS"
        )
        td_browser_operation_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_BROWSER_OPERATION_TIMEOUT_SECONDS",
                str(DEFAULT_TD_BROWSER_OPERATION_TIMEOUT_SECONDS),
            ),
            key="TD_BROWSER_OPERATION_TIMEOUT_SECONDS",
        )
        td_leads_browser_operation_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS",
                str(DEFAULT_TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS),
            ),
            key="TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS",
        )
        td_leads_browser_cleanup_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS",
                str(DEFAULT_TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS),
            ),
            key="TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS",
        )
        td_leads_store_worker_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS",
                str(DEFAULT_TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS),
            ),
            key="TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS",
        )
        td_leads_gather_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_LEADS_GATHER_TIMEOUT_SECONDS",
                str(DEFAULT_TD_LEADS_GATHER_TIMEOUT_SECONDS),
            ),
            key="TD_LEADS_GATHER_TIMEOUT_SECONDS",
        )
        td_leads_cancellation_drain_timeout_seconds = _parse_positive_int(
            db_values.get(
                "TD_LEADS_CANCELLATION_DRAIN_TIMEOUT_SECONDS",
                str(DEFAULT_TD_LEADS_CANCELLATION_DRAIN_TIMEOUT_SECONDS),
            ),
            key="TD_LEADS_CANCELLATION_DRAIN_TIMEOUT_SECONDS",
        )

        td_store_dashboard_path = _clean_text(
            db_values["TD_STORE_DASHBOARD_PATH"], key="TD_STORE_DASHBOARD_PATH"
        )
        reports_root = env_values["REPORTS_ROOT"]
        customer_followup_input_dir = _clean_optional_path_config(
            db_values.get("CUSTOMER_FOLLOWUP_INPUT_DIR"),
            default=str(Path(reports_root) / "inputs" / "customer_followup"),
        )
        customer_followup_external_input_dir = _clean_optional_path_config(
            db_values.get("CUSTOMER_FOLLOWUP_EXTERNAL_INPUT_DIR"),
            default=str(Path(customer_followup_input_dir) / "external_leads"),
        )
        customer_followup_archive_dir = _clean_optional_path_config(
            db_values.get("CUSTOMER_FOLLOWUP_ARCHIVE_DIR"),
            default=str(Path(reports_root) / "archive" / "customer_followup"),
        )
        customer_followup_output_dir = _clean_optional_path_config(
            db_values.get("CUSTOMER_FOLLOWUP_OUTPUT_DIR"),
            default=str(Path(reports_root) / "outputs" / "customer_followup"),
        )
        customer_followup_backlog_warning_threshold = _parse_positive_int(
            db_values.get(
                "CUSTOMER_FOLLOWUP_BACKLOG_WARNING_THRESHOLD",
                str(DEFAULT_CUSTOMER_FOLLOWUP_BACKLOG_WARNING_THRESHOLD),
            ),
            key="CUSTOMER_FOLLOWUP_BACKLOG_WARNING_THRESHOLD",
        )
        td_storage_state_filename = _clean_text(
            db_values["TD_STORAGE_STATE_FILENAME"], key="TD_STORAGE_STATE_FILENAME"
        )

        pdf_render_backend = _clean_text(
            db_values["PDF_RENDER_BACKEND"], key="PDF_RENDER_BACKEND"
        )

        chrome_exec_raw = os.getenv("PDF_RENDER_CHROME_EXECUTABLE", "")
        chrome_exec = chrome_exec_raw.strip() or None

        if pdf_render_backend.lower() == "local_chrome" and not chrome_exec:
            message = (
                "PDF_RENDER_CHROME_EXECUTABLE must be set when PDF_RENDER_BACKEND=local_chrome"
            )
            logger.error(message)
            raise ConfigError(message)

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

        json_log_file = env_values["JSON_LOG_FILE"]

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
            ingest_batch_size=ingest_batch_size,
            report_email_from=report_email_from,
            report_email_smtp_host=report_email_smtp_host,
            report_email_smtp_port=report_email_smtp_port,
            report_email_smtp_username=report_email_smtp_username,
            report_email_smtp_password=report_email_smtp_password,
            report_email_use_tls=report_email_use_tls,
            report_email_send_max_attempts=report_email_send_max_attempts,
            report_email_send_initial_delay_seconds=report_email_send_initial_delay_seconds,
            report_email_send_max_delay_seconds=report_email_send_max_delay_seconds,
            report_email_send_transient_exceptions=report_email_send_transient_exceptions,
            pdf_render_backend=pdf_render_backend,
            pdf_render_headless=pdf_render_headless,
            etl_headless=etl_headless,
            etl_step_timeout_seconds=etl_step_timeout_seconds,
            pdf_render_timeout_seconds=pdf_render_timeout_seconds,
            pipeline_skip_dom_logging=pipeline_skip_dom_logging,
            skip_lead_assignment=skip_lead_assignment,
            uc_ignore_https_errors=uc_ignore_https_errors,
            td_browser_operation_timeout_seconds=td_browser_operation_timeout_seconds,
            td_leads_browser_operation_timeout_seconds=td_leads_browser_operation_timeout_seconds,
            td_leads_browser_cleanup_timeout_seconds=td_leads_browser_cleanup_timeout_seconds,
            td_leads_store_worker_timeout_seconds=td_leads_store_worker_timeout_seconds,
            td_leads_gather_timeout_seconds=td_leads_gather_timeout_seconds,
            td_leads_cancellation_drain_timeout_seconds=td_leads_cancellation_drain_timeout_seconds,
            customer_followup_input_dir=customer_followup_input_dir,
            customer_followup_external_input_dir=customer_followup_external_input_dir,
            customer_followup_archive_dir=customer_followup_archive_dir,
            customer_followup_output_dir=customer_followup_output_dir,
            customer_followup_backlog_warning_threshold=customer_followup_backlog_warning_threshold,
        )


config = Config.load_from_env_and_db()
