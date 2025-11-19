# dashboard_downloader/first_login.py
from __future__ import annotations
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

if __package__ in (None, ""):
    import sys

    _HERE = Path(__file__).resolve().parent
    sys.path.insert(0, str(_HERE.parent))

    from dashboard_downloader.config import HOME_URL, LOGIN_URL, storage_state_path  # type: ignore  # noqa: E402
    from dashboard_downloader import page_selectors as sel  # type: ignore  # noqa: E402
else:
    from .config import HOME_URL, LOGIN_URL, storage_state_path
    from . import page_selectors as sel

def first_login_headed(username: str, password: str) -> None:
    """
    Runs on macOS (headed) once to create a portable storage_state.json
    that can be copied to the Linux server. No full Chrome profile copy needed.
    """
    with sync_playwright() as p:
        # Use system Chrome on mac for maximum OTP success (optional)
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(storage_state_path().parent / "tmp_mac_profile"),
            channel="chrome",
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        page = ctx.new_page()

        # 1) Go to login
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # 2) If already logged in, we should bounce to HOME_URL
        try:
            page.wait_for_url(HOME_URL, timeout=3000)
            already_logged_in = True
        except PWTimeoutError:
            already_logged_in = False

        if not already_logged_in:
            page.wait_for_selector(sel.LOGIN_USERNAME, timeout=15000)
            page.fill(sel.LOGIN_USERNAME, username)
            page.fill(sel.LOGIN_PASSWORD, password)
            page.click(sel.LOGIN_SUBMIT)

            try:
                page.wait_for_url(HOME_URL, timeout=60000)
            except PWTimeoutError:
                print("If OTP is required, complete it now in the browser, then press Enter…")
                input()
                page.wait_for_url(HOME_URL, timeout=60000)

        # 3) Save a portable storage state (cookies + localStorage)
        ctx.storage_state(path=str(storage_state_path()))
        ctx.close()
        print(f"Saved storage state → {storage_state_path()}")

if __name__ == "__main__":
    from app.config import config

    user = config.td_global_username
    pwd = config.td_global_password
    if not user or not pwd:
        raise SystemExit("Missing TD_GLOBAL_USERNAME/TD_GLOBAL_PASSWORD in system_config")
    first_login_headed(user, pwd)
