# File: downloader/utils.py
from pathlib import Path
from typing import Literal
from downloader import page_selectors
from playwright.sync_api import sync_playwright
from . import config

Store = Literal["UN3668", "KN3817"]

def user_pass_for(store: Store) -> tuple[str, str]:
    if store == "UN3668":
        return config.TD_UN3668_USERNAME, config.TD_UN3668_PASSWORD
    return config.TD_KN3817_USERNAME, config.TD_KN3817_PASSWORD

def profile_dir(store: Store) -> Path:
    p = config.PROFILES_DIR / store
    p.mkdir(parents=True, exist_ok=True)
    return p

def downloads_dir(store: Store) -> Path:
    p = config.DATA_DIR / store
    p.mkdir(parents=True, exist_ok=True)
    return p

def first_login(store: Store, headless: bool = False) -> None:
    """First-time interactive/OTP-capable login using persistent profile."""
    user, pwd = user_pass_for(store)
    if not user or not pwd:
        raise RuntimeError(f"Missing creds for store={store}")

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir(store)),
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        page = ctx.new_page()
        page.goto(config.LOGIN_URL, wait_until="domcontentloaded")

        # Fill and submit login
        page.fill(page_selectors.LOGIN_USERNAME, user)
        page.fill(page_selectors.LOGIN_PASSWORD, pwd)
        page.click(page_selectors.LOGIN_SUBMIT)

        # If OTP page appears, the site will prompt you.
        # Complete OTP manually in this run (set headless=False for first time).
        page.wait_for_load_state("networkidle")

        # Land on dashboard to warm cookies
        page.goto(config.DASHBOARD_URL, wait_until="networkidle")
        ctx.storage_state(path=str(profile_dir(store) / "storage_state.json"))
        ctx.close()

def run_downloads(store: Store, headless: bool = True) -> None:
    """Use existing authenticated profile to fetch dashboard and click all download links."""
    dl_dir = downloads_dir(store)

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir(store)),
            headless=headless,
            accept_downloads=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        page = ctx.new_page()
        page.goto(config.DASHBOARD_URL, wait_until="networkidle")

        # Example: click all download links found on dashboard
        links = page.locator(page_selectors.DOWNLOAD_LINKS)
        count = links.count()
        for i in range(count):
            with page.expect_download() as dl_info:
                links.nth(i).click()
            download = dl_info.value
            download.save_as(str(dl_dir / download.suggested_filename))
        ctx.close()

# Optional CLI shims wired in pyproject [project.scripts]
def cli_first_login():
    import argparse
    from . import page_selectors  # ensure module is importable

    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["UN3668", "KN3817"], required=True)
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()
    first_login(args.store, headless=args.headless)

def cli_run():
    import argparse
    from . import page_selectors

    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["UN3668", "KN3817"], required=True)
    ap.add_argument("--headed", action="store_true")
    args = ap.parse_args()
    run_downloads(args.store, headless=not args.headed)
