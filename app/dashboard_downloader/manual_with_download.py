import asyncio
from collections import defaultdict
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright, Page, TimeoutError

# -----------------------------------------------------------------------------
# BASIC CONSTANTS (from your config / .env)
# -----------------------------------------------------------------------------

TD_BASE_URL = "https://simplifytumbledry.in"
TD_HOME_URL = "https://simplifytumbledry.in/home"
TD_LOGIN_URL = "https://simplifytumbledry.in/home/login"

TMS_BASE = "https://tms.simplifytumbledry.in"
TD_STORE_DASHBOARD_PATH = "/mis/partner_dashboard?store_code={store_code}"

# YMD for merged filenames
YMD_TODAY = datetime.now().strftime("%Y%m%d")

# -----------------------------------------------------------------------------
# FILE_SPECS + MERGED_NAMES (copied from your snippet)
# -----------------------------------------------------------------------------

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

MERGED_NAMES = {
    "missed_leads": f"merged_missed_leads_{YMD_TODAY}.csv",
    "undelivered_all": f"merged_undelivered_all_{YMD_TODAY}.csv",
    "repeat_customers": f"merged_repeat_customers_{YMD_TODAY}.csv",
}

# -----------------------------------------------------------------------------
# CREDENTIALS & STORES
# -----------------------------------------------------------------------------

# Login once as A668 (UN3668)
TD_USERNAME = "A668"
TD_PASSWORD = "Wagid@321"

# Stores to hit in a single TMS session
STORES = ["A668", "A817", "A526"]

# -----------------------------------------------------------------------------
# SELECTORS
# -----------------------------------------------------------------------------

LOGIN_USERNAME = "#txtUserId"
LOGIN_PASSWORD = "#txtPassword"
LOGIN_STORE_CODE = "#txtBranchPin"
LOGIN_SUBMIT = "#btnLogin, button:has-text('Login')"

# Daily Ops Tracker card heading:
# <h5 class="card-title">Daily Operations Tracker</h5>
DAILY_OPS_CARD_LOCATOR = "h5.card-title"

# -----------------------------------------------------------------------------
# OUTPUT DIRECTORY (for this manual script)
# -----------------------------------------------------------------------------

DATA_ROOT = Path("./_single_session_downloads")
DATA_ROOT.mkdir(exist_ok=True)

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

async def download_file_for_spec(
    tms_page: Page,
    store_code: str,
    spec: dict,
) -> Path | None:
    """
    Use Playwright's request client to download CSV for a single spec & store.
    Returns the path of the downloaded file, or None if download=False or failure.
    """
    if not spec.get("download", False):
        return None

    url_template = spec["url_template"]
    out_name_template = spec["out_name_template"]

    url = url_template.format(sc=store_code)
    out_name = out_name_template.format(sc=store_code)

    store_dir = DATA_ROOT / store_code
    store_dir.mkdir(exist_ok=True, parents=True)
    out_path = store_dir / out_name

    print(f"  [store {store_code}] downloading {spec['key']} from {url}")

    resp = await tms_page.context.request.get(url)
    status = resp.status
    if status != 200:
        print(f"  [store {store_code}] !! HTTP {status} for {spec['key']} -> skipping")
        return None

    body = await resp.body()
    out_path.write_bytes(body)
    print(f"  [store {store_code}] saved to {out_path}")

    return out_path


def merge_bucket_files(bucket: str, files: list[Path]) -> Path | None:
    """
    Naive CSV merge:
      - Take header from first file
      - Append data rows (skip header) from subsequent files
    Writes into DATA_ROOT / MERGED_NAMES[bucket].
    """
    if not files:
        return None

    merged_name = MERGED_NAMES.get(bucket)
    if not merged_name:
        print(f"[merge:{bucket}] No MERGED_NAMES entry, skipping.")
        return None

    merged_path = DATA_ROOT / merged_name
    print(f"[merge:{bucket}] Merging {len(files)} files into {merged_path}")

    with merged_path.open("wb") as out_f:
        first = True
        for f in files:
            data = f.read_bytes()
            if first:
                # Write full first file including header
                out_f.write(data)
                first = False
            else:
                # Skip first line (header) of subsequent files
                try:
                    header_end = data.index(b"\n")
                    out_f.write(data[header_end + 1 :])
                except ValueError:
                    # No newline in file; just skip whole thing
                    pass

    print(f"[merge:{bucket}] Done.")
    return merged_path

# -----------------------------------------------------------------------------
# MAIN FLOW
# -----------------------------------------------------------------------------

async def main() -> None:
    async with async_playwright() as p:
        # Persistent Chrome context – mimics your real behaviour
        context = await p.chromium.launch_persistent_context(
            user_data_dir="./_manual_profile",
            headless=False,
            slow_mo=300,
            channel="chrome",
        )

        pages = context.pages
        page: Page = pages[0] if pages else await context.new_page()

        # 1) LOGIN TO SIMPLIFY
        print(f"Going to login page: {TD_LOGIN_URL}")
        await page.goto(TD_LOGIN_URL, wait_until="domcontentloaded")

        print("Filling login form...")
        await page.wait_for_selector(LOGIN_USERNAME, timeout=30_000)
        await page.fill(LOGIN_USERNAME, TD_USERNAME)
        await page.fill(LOGIN_PASSWORD, TD_PASSWORD)
        await page.fill(LOGIN_STORE_CODE, STORES[0])
        await page.click(LOGIN_SUBMIT)

        print("Waiting to land on home...")
        await page.wait_for_url(TD_HOME_URL + "*", timeout=60_000)
        print(f"Now at: {page.url}")

        await page.wait_for_timeout(2000)

        # 2) CLICK DAILY OPS TRACKER → NEW TMS TAB
        print("Searching for Daily Operations Tracker card...")
        tracker = page.locator(
            DAILY_OPS_CARD_LOCATOR,
            has_text="Daily Operations Tracker",
        )
        await tracker.wait_for(state="visible", timeout=30_000)
        print("Found Daily Operations Tracker heading, clicking and expecting new TMS page...")

        try:
            async with context.expect_page() as page_info:
                await tracker.click()
            tms_page: Page = await page_info.value
            await tms_page.wait_for_load_state("domcontentloaded")
            print(f"TMS page opened in NEW tab: {tms_page.url}")
        except TimeoutError:
            print("No new page detected, falling back to same-tab navigation...")
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                await tracker.click()
            tms_page = page
            print(f"TMS page opened in SAME tab: {tms_page.url}")

        await tms_page.wait_for_timeout(2000)

        # 3) LOOP STORES → DASHBOARD + DOWNLOAD FILE_SPECS
        bucket_files: dict[str, list[Path]] = defaultdict(list)

        for store_code in STORES:
            dashboard_url = f"{TMS_BASE}{TD_STORE_DASHBOARD_PATH.format(store_code=store_code)}"
            print(f"\n=== Store {store_code}: going to dashboard ===")
            print(f"Dashboard URL: {dashboard_url}")
            await tms_page.goto(dashboard_url, wait_until="domcontentloaded")
            print(f"Now at: {tms_page.url}")
            await tms_page.wait_for_timeout(2000)

            for spec in FILE_SPECS:
                path = await download_file_for_spec(tms_page, store_code, spec)
                if not path:
                    continue

                bucket = spec.get("merge_bucket")
                if bucket:
                    bucket_files[bucket].append(path)

        # 4) MERGE BUCKETS
        print("\n=== Merging buckets ===")
        for bucket, files in bucket_files.items():
            merge_bucket_files(bucket, files)

        print("\nAll stores processed and merges complete. Closing browser...")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
