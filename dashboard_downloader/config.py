# File: dashboard_downloader/config.py
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List

# ── Paths ────────────────────────────────────────────────────────────────────
PKG_ROOT = Path(__file__).resolve().parent            # .../simplify_downloader/dashboard_downloader
PROJECT_ROOT = PKG_ROOT.parent                        # .../simplify_downloader

ENV_PATH = PROJECT_ROOT / ".env"                      # keep .env at project root
PROFILES_DIR = PKG_ROOT / "profiles"                  # .../dashboard_downloader/profiles
DATA_DIR = PKG_ROOT / "data"                          # .../dashboard_downloader/data
PROFILES_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

def storage_state_path(filename: str | None = None) -> Path:
    """Return the path to the shared Playwright storage state JSON.

    Parameters
    ----------
    filename:
        Override the default storage state filename.  When omitted we look for
        ``TD_STORAGE_STATE_FILENAME`` in the environment and fall back to a
        sensible default.
    
    Notes
    -----
    The file lives inside :data:`PROFILES_DIR` so that it travels with the
    other migratable browser artefacts when copying between machines.
    """

    name = filename or os.getenv("TD_STORAGE_STATE_FILENAME", "storage_state.json")
    path = PROFILES_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

# Load env
load_dotenv(ENV_PATH)

# ── Storage state helpers ───────────────────────────────────────────────────

def _storage_state_filename_for(store_key: str) -> str | None:
    env_var = f"TD_{store_key}_STORAGE_STATE_FILENAME"
    return os.getenv(env_var)


def storage_state_for_store(store_key: str) -> Path:
    """Return the storage_state path for a given store.

    Allows overriding the default filename via ``TD_<STORE>_STORAGE_STATE_FILENAME``.
    When unset, falls back to :func:`storage_state_path` with its default naming.
    """

    override = _storage_state_filename_for(store_key)
    if override:
        return storage_state_path(override)
    return storage_state_path()


# ── URLs ─────────────────────────────────────────────────────────────────────
BASE_WEB = "https://simplifytumbledry.in"
LOGIN_URL = f"{BASE_WEB}/home/login"
HOME_URL  = f"{BASE_WEB}/home"

TMS_BASE = os.getenv("TMS_BASE", "https://tms.simplifytumbledry.in").rstrip("/")
TD_STORE_DASHBOARD_PATH = os.getenv("TD_STORE_DASHBOARD_PATH", "/mis/partner_dashboard?store_code={store_code}")

def tms_dashboard_url(store_code: str) -> str:
    return f"{TMS_BASE}{TD_STORE_DASHBOARD_PATH.format(store_code=store_code)}"

# ── Credentials (from .env) ─────────────────────────────────────────────────
TD_UN3668_USERNAME = os.getenv("TD_UN3668_USERNAME", "")
TD_UN3668_PASSWORD = os.getenv("TD_UN3668_PASSWORD", "")
TD_KN3817_USERNAME = os.getenv("TD_KN3817_USERNAME", "")
TD_KN3817_PASSWORD = os.getenv("TD_KN3817_PASSWORD", "")

# Store codes (actual TMS store codes, not human codes)
TD_UN3668_STORE_CODE = os.getenv("TD_UN3668_STORE_CODE", "A668")
TD_KN3817_STORE_CODE = os.getenv("TD_KN3817_STORE_CODE", "A817")

# ── Store registry ───────────────────────────────────────────────────────────


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
        cfg["storage_state"] = storage_state_for_store(profile_label)

    return cfg


def _known_store(store_key: str, *, username: str, password: str, store_code: str) -> Dict[str, Any]:
    base = {
        "username": username,
        "password": password,
        "profile_dir": PROFILES_DIR / store_key,
        "storage_state": storage_state_for_store(store_key),
    }
    return _apply_store_defaults(base, store_code=store_code, profile_key=store_key)


STORES = {
    "UN3668": _known_store(
        "UN3668",
        username=TD_UN3668_USERNAME,
        password=TD_UN3668_PASSWORD,
        store_code=TD_UN3668_STORE_CODE,
    ),
    "KN3817": _known_store(
        "KN3817",
        username=TD_KN3817_USERNAME,
        password=TD_KN3817_PASSWORD,
        store_code=TD_KN3817_STORE_CODE,
    ),
}


DEFAULT_STORE_CODES = [cfg["store_code"] for cfg in STORES.values()]


def _find_known_store_by_code(store_code: str) -> str | None:
    normalized = store_code.strip().upper()
    for name, cfg in STORES.items():
        if cfg.get("store_code", "").upper() == normalized:
            return name
    return None


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

        if normalized in STORES:
            cfg = dict(STORES[normalized])
            resolved[cfg["store_code"]] = cfg
            continue

        alias = _find_known_store_by_code(normalized)
        if alias:
            cfg = dict(STORES[alias])
            resolved[cfg["store_code"]] = cfg
            continue

        cfg = _apply_store_defaults({}, store_code=normalized, profile_key=normalized)
        resolved[cfg["store_code"]] = cfg

    return resolved


def env_stores_list() -> List[str]:
    raw = os.getenv("stores_list") or os.getenv("STORES_LIST") or ""
    return [part.strip() for part in raw.split(",") if part.strip()]

# ── File specs from HAR (label + url template + filename + flags) ────────────
#  - key: stable identifier for logging
#  - url_template: use {sc} to inject store_code
#  - out_name_template: may use {sc} (store code) and {ymd} (YYYYMMDD)
#  - download: toggle per-link
#  - merge_bucket: set a bucket name (e.g., "missed_leads") to include in a later merge
YMD_TODAY = datetime.now().strftime("%Y%m%d")

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

# Output name for merged buckets
MERGED_NAMES = {
    "missed_leads": f"merged_missed_leads_{YMD_TODAY}.csv",
    "undelivered_all": f"merged_undelivered_all_{YMD_TODAY}.csv",
    "repeat_customers": f"merged_repeat_customers_{YMD_TODAY}.csv",
}

# Each key == a merge_bucket value.
MERGE_BUCKET_DB_SPECS = {
    "missed_leads": {
        "table_name": "missed_leads",
        # dedupe by store_code + mobile_number per upsert requirements.
        "dedupe_keys": ["store_code", "mobile_number"],
        # Columns that must contain values for the row to be ingested.
        "required_columns": ["pickup_row_id", "store_code", "mobile_number"],
        "column_map": {
            ("id", "pickup_row_id", "pickup row id"): "pickup_row_id",  # numeric id in CSV
            "mobile_number": "mobile_number",
            "pickup_no": "pickup_no",
            "pickup_created_date": "pickup_created_date",
            "pickup_created_time": "pickup_created_time",
            "store_code": "store_code",
            "store_name": "store_name",
            "pickup_date": "pickup_date",
            "pickup_time": "pickup_time",
            "customer_name": "customer_name",
            "special_instruction": "special_instruction",
            "source": "source",
            "final_source": "final_source",
            "customer_type": "customer_type",
            "is_order_placed": "is_order_placed",
        },
        "coerce": {
            "pickup_row_id": "int",        # safe to keep int; change to "str" if IDs can exceed bigint
            "mobile_number": "str",        # keep phone as TEXT to preserve formatting
            "pickup_no": "str",
            "pickup_created_date": "date",
            "pickup_created_time": "str",  # keep as text; or add "time" support if you extend coercer
            "store_code": "str",
            "store_name": "str",
            "pickup_date": "date",
            "pickup_time": "str",
            "customer_name": "str",
            "special_instruction": "str",
            "source": "str",
            "final_source": "str",
            "customer_type": "str",
            "is_order_placed": "bool", 
        },
    },

    "undelivered_all": {
        "table_name": "undelivered_orders",
        # order_id uniquely identifies the record across stores.
        "dedupe_keys": ["store_code", "order_id"],
        "required_columns": ["order_id"],
        "column_map": {
            ("order_id", "order_no"): "order_id",
            "order_date": "order_date",
            "store_code": "store_code",
            "store_name": "store_name",
            "taxable_amount": "taxable_amount",
            "net_amount": "net_amount",
            "service_code": "service_code",
            "mobile_no": "mobile_no",
            "status": "status",
            "customer_id": "customer_id",
            "expected_deliver_on": "expected_deliver_on",
            "actual_deliver_on": "actual_deliver_on",
        },
        "coerce": {
            "order_id": "str",
            "order_date": "date",
            "store_code": "str",
            "store_name": "str",
            "taxable_amount": "float",
            "net_amount": "float",
            "service_code": "str",
            "mobile_no": "str",
            "status": "str",
            "customer_id": "str",
            "expected_deliver_on": "date",
            "actual_deliver_on": "date",
        },
    },

    "repeat_customers": {
        "table_name": "repeat_customers",
        # Only three columns; dedupe on store+mobile. Status is 'Yes' now but may change.
        "dedupe_keys": ["store_code", "mobile_no"],
        "required_columns": ["store_code", "mobile_no"],
        "column_map": {
            "Store Code": "store_code",
            "Mobile No.": "mobile_no",
            "Status": "status",
        },
        "coerce": {
            "store_code": "str",
            "mobile_no": "str",   # CSV parsed as int, but store as TEXT to avoid issues
            "status": "str",
        },
    },
}
