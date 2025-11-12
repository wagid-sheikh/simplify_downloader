# File: downloader/config.py
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────────
PKG_ROOT = Path(__file__).resolve().parent            # .../simplify_downloader/downloader
PROJECT_ROOT = PKG_ROOT.parent                        # .../simplify_downloader

ENV_PATH = PROJECT_ROOT / ".env"                      # keep .env at project root
PROFILES_DIR = PKG_ROOT / "profiles"                  # .../downloader/profiles
DATA_DIR = PKG_ROOT / "data"                          # .../downloader/data
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Load env
load_dotenv(ENV_PATH)

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
STORES = {
    "UN3668": {
        "username": TD_UN3668_USERNAME,
        "password": TD_UN3668_PASSWORD,
        "store_code": TD_UN3668_STORE_CODE,
        "profile_dir": PROFILES_DIR / "UN3668",
        "dashboard_url": tms_dashboard_url(TD_UN3668_STORE_CODE),
    },
    "KN3817": {
        "username": TD_KN3817_USERNAME,
        "password": TD_KN3817_PASSWORD,
        "store_code": TD_KN3817_STORE_CODE,
        "profile_dir": PROFILES_DIR / "KN3817",
        "dashboard_url": tms_dashboard_url(TD_KN3817_STORE_CODE),
    },
}

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
        "download": True,
        "merge_bucket": "missed_leads",
    },
    {
        "key": "undelivered_last10",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=u10&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-last10.csv",
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "undelivered_u",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=u&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-u.csv",
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "undelivered_all",
        "url_template": f"{TMS_BASE}/mis/download_undelivered_csv?type=a&store_code={{sc}}",
        "out_name_template": "{sc}-undelivered-all.csv",
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "repeat_customers",
        "url_template": f"{TMS_BASE}/mis/download_repeat_csv?store_code={{sc}}",
        "out_name_template": "{sc}-repeat-customers.csv",
        "download": True,
        "merge_bucket": None,
    },
    {
        "key": "nonpackage_all",
        "url_template": f"{TMS_BASE}/mis/download_nonpackageorder_csv?type=all&store_code={{sc}}",
        "out_name_template": "{sc}-non-package-all.csv",
        "download": False,
        "merge_bucket": None,
    },
    {
        "key": "nonpackage_u",
        "url_template": f"{TMS_BASE}/mis/download_nonpackageorder_csv?type=u&store_code={{sc}}",
        "out_name_template": "{sc}-non-package-u.csv",
        "download": False,
        "merge_bucket": None,
    },
]

# Output name for merged buckets
MERGED_NAMES = {
    "missed_leads": f"merged_missed_leads_{YMD_TODAY}.csv",
}
