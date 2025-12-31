import asyncio
from playwright.async_api import async_playwright, Page, TimeoutError

TD_HOME_URL = "https://simplifytumbledry.in/home"
TD_LOGIN_URL = "https://simplifytumbledry.in/home/login"
TMS_BASE = "https://tms.simplifytumbledry.in"
TD_STORE_DASHBOARD_PATH = "/mis/partner_dashboard?store_code={store_code}"

TD_USERNAME = "A668"
TD_PASSWORD = "Wagid@321"

STORES = ["A668", "A817", "A526"]

LOGIN_USERNAME = "#txtUserId"
LOGIN_PASSWORD = "#txtPassword"
LOGIN_STORE_CODE = "#txtBranchPin"
LOGIN_SUBMIT = "#btnLogin, button:has-text('Login')"


async def main() -> None:
    async with async_playwright() as p:
        # Use a persistent Chrome profile so behaviour is close to real Chrome
        context = await p.chromium.launch_persistent_context(
            user_data_dir="./_manual_profile",
            headless=False,
            slow_mo=300,
            channel="chrome",
        )

        pages = context.pages
        page: Page = pages[0] if pages else await context.new_page()

        # 1) Login to Simplify
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

        # little pause to let cards render
        await page.wait_for_timeout(2000)

        # 2) Find the <h5 class="card-title">Daily Operations Tracker</h5>
        print("Searching for Daily Operations Tracker card...")
        tracker = page.locator("h5.card-title", has_text="Daily Operations Tracker")
        await tracker.wait_for(state="visible", timeout=30_000)
        print("Found Daily Operations Tracker heading, clicking and expecting new TMS page...")

        tms_page: Page

        try:
            # Normal behaviour: new tab/window opens
            async with context.expect_page() as page_info:
                await tracker.click()
            tms_page = await page_info.value
            await tms_page.wait_for_load_state("domcontentloaded")
            print(f"TMS page opened in NEW tab: {tms_page.url}")
        except TimeoutError:
            # Fallback: if for some reason it opens in same tab
            print("No new page detected, falling back to same-tab navigation...")
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                await tracker.click()
            tms_page = page
            print(f"TMS page opened in SAME tab: {tms_page.url}")

        await tms_page.wait_for_timeout(2000)

        # 3) Visit dashboards for multiple stores in SAME TMS SESSION
        for store_code in STORES:
            target_url = f"{TMS_BASE}{TD_STORE_DASHBOARD_PATH.format(store_code=store_code)}"
            print(f"\n=== Switching to store {store_code} ===")
            print(f"Going to: {target_url}")

            await tms_page.goto(target_url, wait_until="domcontentloaded")
            print(f"Now at: {tms_page.url}")

            # Pause so you can see if it's logged in / correct store vs "need to be logged in"
            await tms_page.wait_for_timeout(5000)

        print("\nAll stores visited in a single TMS session. Closing browser...")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
