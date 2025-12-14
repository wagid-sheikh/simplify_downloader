# File: dashboard_downloader/config.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import sqlalchemy as sa

from app.common.dashboard_store import store_master
from app.common.db import session_scope
from app.config import config

# ── Paths ────────────────────────────────────────────────────────────────────
PKG_ROOT = Path(__file__).resolve().parent            # .../app/dashboard_downloader
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


def normalize_store_codes(values: Iterable[str]) -> List[str]:
    return sorted({token.strip().upper() for token in values if token and token.strip()})


async def fetch_store_codes(
    *,
    database_url: str,
    etl_flag: bool | None = None,
    report_flag: bool | None = None,
    store_codes: Sequence[str] | None = None,
) -> List[str]:
    filters = []
    if etl_flag is not None:
        filters.append(store_master.c.etl_flag.is_(etl_flag))
    if report_flag is not None:
        filters.append(store_master.c.report_flag.is_(report_flag))

    normalized_codes = normalize_store_codes(store_codes or [])
    async with session_scope(database_url) as session:
        stmt = sa.select(sa.func.upper(store_master.c.store_code)).where(store_master.c.is_active.is_(True))
        if filters:
            stmt = stmt.where(sa.and_(*filters))
        if normalized_codes:
            stmt = stmt.where(sa.func.upper(store_master.c.store_code).in_(normalized_codes))
        result = await session.execute(stmt)
        return sorted({row[0] for row in result})


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


# Compatibility constant (no default stores in the new single-session model)
DEFAULT_STORE_CODES: List[str] = []

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
        "delete_source_after_ingest": True,
        "download": True,
        "merge_bucket": "nonpackage_all",
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
    "nonpackage_all": f"merged_nonpackage_all_{YMD_TODAY}.csv",
}

# Each key == a merge_bucket value.
MERGE_BUCKET_DB_SPECS = {
    "missed_leads": {
        "table_name": "missed_leads",
        # dedupe by store_code + mobile_number per upsert requirements.
        "dedupe_keys": ["store_code", "mobile_number"],
        # Columns that must contain values for the row to be ingested.
        "required_columns": [
            "pickup_row_id",
            "store_code",
            "mobile_number",
            "run_id",
            "run_date",
        ],
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
            "run_id": "str",
            "run_date": "date",
        },
    },

    "undelivered_all": {
        "table_name": "undelivered_orders",
        # order_id uniquely identifies the record across stores.
        "dedupe_keys": ["store_code", "order_id"],
        "required_columns": ["order_id", "run_id", "run_date"],
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
            "run_id": "str",
            "run_date": "date",
        },
    },

    "repeat_customers": {
        "table_name": "repeat_customers",
        # Only three columns; dedupe on store+mobile. Status is 'Yes' now but may change.
        "dedupe_keys": ["store_code", "mobile_no"],
        "required_columns": ["store_code", "mobile_no", "run_id", "run_date"],
        "insert_only": True,
        "column_map": {
            "Store Code": "store_code",
            "Mobile No.": "mobile_no",
            "Status": "status",
        },
        "coerce": {
            "store_code": "str",
            "mobile_no": "str",   # CSV parsed as int, but store as TEXT to avoid issues
            "status": "str",
            "run_id": "str",
            "run_date": "date",
        },
    },

    "nonpackage_all": {
        "table_name": "nonpackage_orders",
        # Deduplicate by store and mobile number for customer-level updates.
        "dedupe_keys": ["store_code", "mobile_no"],
        "required_columns": ["store_code", "mobile_no", "order_date", "run_id", "run_date"],
        "insert_only": True,
        "column_map": {
            "Store Code": "store_code",
            "Store Name": "store_name",
            ("Mobile No", "Mobile No.", "Mobile"): "mobile_no",
            "Taxable Amount": "taxable_amount",
            "Order Date": "order_date",
            "Expected Delivery Date": "expected_delivery_date",
            "Actual Delivery Date": "actual_delivery_date",
        },
        "coerce": {
            "store_code": "str",
            "store_name": "str",
            "mobile_no": "str",
            "taxable_amount": "float",
            "order_date": "date",
            "expected_delivery_date": "date",
            "actual_delivery_date": "date",
            "run_id": "str",
            "run_date": "date",
        },
    },
}
