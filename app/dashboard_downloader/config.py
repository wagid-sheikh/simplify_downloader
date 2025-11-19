# File: dashboard_downloader/config.py
from pathlib import Path
from typing import Any, Dict, Iterable, List

from app.config import config
from .merge_specs import MERGE_BUCKET_DB_SPECS, MERGED_NAMES

# ── Paths ────────────────────────────────────────────────────────────────────
PKG_ROOT = Path(__file__).resolve().parent            # .../simplify_downloader/dashboard_downloader
PROFILES_DIR = PKG_ROOT / "profiles"                  # .../dashboard_downloader/profiles
DATA_DIR = PKG_ROOT / "data"                          # .../dashboard_downloader/data
PROFILES_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


def storage_state_path(filename: str | None = None) -> Path:
    """Return the path to the single shared Playwright storage state JSON."""

    name = filename or config.td_storage_state_filename
    path = PROFILES_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


# ── URLs ─────────────────────────────────────────────────────────────────────
TD_BASE_URL = config.td_base_url
TD_LOGIN_URL = config.td_login_url
TD_HOME_URL = config.td_home_url

LOGIN_URL = TD_LOGIN_URL
HOME_URL = TD_HOME_URL
TMS_BASE = config.tms_base
TD_STORE_DASHBOARD_PATH = config.td_store_dashboard_path

def tms_dashboard_url(store_code: str) -> str:
    return f"{TMS_BASE}{TD_STORE_DASHBOARD_PATH.format(store_code=store_code)}"

# ── Credentials ─────────────────────────────────────────────────────────────

def _apply_store_defaults(
    base: Dict[str, Any], *, store_code: str, profile_key: str | None = None
) -> Dict[str, Any]:
    """Return a copy of ``base`` with derived defaults for dashboard access."""

    normalized_code = store_code.strip().upper()
    profile_label = (profile_key or normalized_code).strip() or normalized_code

    cfg: Dict[str, Any] = dict(base)
    cfg["store_code"] = normalized_code
    cfg.setdefault("home_url", HOME_URL)
    cfg.setdefault("login_url", LOGIN_URL)
    cfg.setdefault("dashboard_url", tms_dashboard_url(normalized_code))
    cfg.setdefault("profile_key", profile_label)

    profile_dir = cfg.get("profile_dir")
    if profile_dir is None:
        profile_dir_path = PROFILES_DIR / profile_label
    else:
        profile_dir_path = Path(profile_dir)
    cfg["profile_dir"] = profile_dir_path

    if not cfg.get("storage_state"):
        cfg["storage_state"] = storage_state_path()

    return cfg


def global_credentials() -> tuple[str, str]:
    """Return the global CRM username/password pair."""

    return config.td_global_username.strip(), config.td_global_password.strip()


def stores_from_list(store_ids: Iterable[str]) -> Dict[str, dict]:
    """Resolve store configurations for the provided identifiers or codes."""

    resolved: Dict[str, dict] = {}
    for raw in store_ids:
        if not raw:
            continue
        token = raw.strip()
        if not token:
            continue

        normalized = token.upper()
        cfg = _apply_store_defaults({}, store_code=normalized, profile_key=normalized)
        resolved[cfg["store_code"]] = cfg

    return resolved


def env_stores_list() -> List[str]:
    return list(config.stores_list)


# Compatibility constant (no default stores in the new single-session model)
DEFAULT_STORE_CODES: List[str] = []

# ── File specs from HAR (label + url template + filename + flags) ────────────
#  - key: stable identifier for logging
#  - url_template: use {sc} to inject store_code
#  - out_name_template: may use {sc} (store code) and {ymd} (YYYYMMDD)
#  - download: toggle per-link
#  - merge_bucket: set a bucket name (e.g., "missed_leads") to include in a later merge

FILE_SPECS = [
    {
        "key": "missed_leads",
        "url_template": f"{TMS_BASE}/mis/download_csv?store_code={{sc}}",
        "out_name_template": "{sc}-missed-leads.csv",
        "delete_source_after_ingest": True,
        "download": True,
        "merge_bucket": "missed_leads",
    },
    {
        "key": "undelivered_last10",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=u10&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-last10.csv",
        "delete_source_after_ingest": False,
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "undelivered_u",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=u&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-u.csv",
        "delete_source_after_ingest": False,
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "undelivered_all",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=a&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-all.csv",
        "delete_source_after_ingest": False,
        "download": True,
        "merge_bucket": "undelivered_all",
    },
    {
        "key": "repeat_customers",
        "url_template": f"{TMS_BASE}/mis/download_repeat_csv?store_code={{sc}}",
        "out_name_template": "{sc}-repeat-customers.csv",
        "delete_source_after_ingest": False,
        "download": True,
        "merge_bucket": "repeat_customers",
    },
    {
        "key": "nonpackage_all",
        "url_template": f"{TMS_BASE}/mis/download_nonpackageorder_csv?type=all&store_code={{sc}}",
        "out_name_template": "{sc}-non-package-all.csv",
        "delete_source_after_ingest": False,
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "nonpackage_u",
        "url_template": f"{TMS_BASE}/mis/download_nonpackageorder_csv?type=u&store_code={{sc}}",
        "out_name_template": "{sc}-non-package-u.csv",
        "delete_source_after_ingest": False,
        "download": False,
        "merge_bucket": None,
    },
]
