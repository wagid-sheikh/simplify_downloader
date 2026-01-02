import asyncio
import os
import re
import sys
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


START_URL = "https://charmswiki.com/pages/allpages"

# Required output folder: current directory / Wiki-PDF / *.pdf
OUT_DIR = os.path.join(os.getcwd(), "Wiki-PDF")

NAV_TIMEOUT_MS = 45_000
RETRY_COUNT = 2

# For validation: save only 2 PDFs
MAX_TO_SAVE = 2


def chrome_executable_path() -> str:
    """
    Prefer explicit override, else use standard macOS Chrome path.
    You can set:
      CHROME_PATH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    """
    env = os.environ.get("CHROME_PATH")
    if env and os.path.exists(env):
        return env

    candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p

    raise FileNotFoundError(
        "Could not find a local Chrome/Canary/Chromium executable. "
        "Install Google Chrome, or set CHROME_PATH to the full executable path."
    )


def sanitize_filename(name: str, max_len: int = 140) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"[\/\\\:\*\?\"\<\>\|\n\r\t]", "-", name)
    name = re.sub(r"[\u0000-\u001f]", "", name)
    name = name.strip(" .-_")
    if not name:
        name = "untitled"
    if len(name) > max_len:
        name = name[:max_len].rstrip(" .-_")
    return name


def is_same_site(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


async def collect_links(page) -> list[str]:
    await page.goto(START_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_timeout(1000)

    hrefs = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
    )

    abs_urls = []
    for href in hrefs:
        if href.startswith("#") or href.lower().startswith(("javascript:", "mailto:")):
            continue

        u = urljoin(START_URL, href)
        if not u.startswith(("http://", "https://")):
            continue
        if not is_same_site(u, START_URL):
            continue
        if u.rstrip("/") == START_URL.rstrip("/"):
            continue

        abs_urls.append(u)

    # Deduplicate preserve-order
    seen = set()
    deduped = []
    for u in abs_urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    return deduped


async def save_page_pdf(context, url: str, idx: int, total: int):
    """
    Open a single page and save as PDF to ./Wiki-PDF/
    """
    for attempt in range(RETRY_COUNT + 1):
        page = await context.new_page()
        try:
            print(f"[{idx}/{total}] Opening: {url}")
            await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)

            title = (await page.title()) or "untitled"
            safe_title = sanitize_filename(title)

            pdf_path = os.path.join(OUT_DIR, f"{safe_title}.pdf")
            if os.path.exists(pdf_path):
                print(f"[{idx}/{total}] SKIP (exists): {os.path.relpath(pdf_path, os.getcwd())}")
                await page.close()
                return ("SKIP", url, pdf_path)

            await page.pdf(
                path=pdf_path,
                format="A4",
                print_background=True,
                margin={"top": "12mm", "right": "12mm", "bottom": "12mm", "left": "12mm"},
                prefer_css_page_size=True,
            )

            print(f"[{idx}/{total}] OK   Saved: {os.path.relpath(pdf_path, os.getcwd())}")
            await page.close()
            return ("OK", url, pdf_path)

        except PlaywrightTimeoutError:
            await page.close()
            if attempt >= RETRY_COUNT:
                print(f"[{idx}/{total}] TIMEOUT: {url}")
                return ("TIMEOUT", url, "")
            print(f"[{idx}/{total}] Timeout, retrying ({attempt+1}/{RETRY_COUNT})...")
            await asyncio.sleep(1.5)

        except Exception as e:
            await page.close()
            if attempt >= RETRY_COUNT:
                print(f"[{idx}/{total}] ERROR: {url} => {type(e).__name__}: {e}")
                return ("ERROR", url, f"{type(e).__name__}: {e}")
            print(f"[{idx}/{total}] Error, retrying ({attempt+1}/{RETRY_COUNT})... => {type(e).__name__}: {e}")
            await asyncio.sleep(1.5)


async def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"PDF output directory: {OUT_DIR}")

    chrome_path = chrome_executable_path()
    print(f"Using local Chrome executable: {chrome_path}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            executable_path=chrome_path,
            args=[
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context()

        index_page = await context.new_page()
        links = await collect_links(index_page)
        await index_page.close()

        total_found = len(links)
        to_save = min(MAX_TO_SAVE, total_found)

        print(f"Found {total_found} candidate links.")
        print(f"Will save {to_save} PDFs (validation run).")

        results = []
        for i, url in enumerate(links[:to_save], start=1):
            res = await save_page_pdf(context, url, i, to_save)
            results.append(res)

        await context.close()
        await browser.close()

        ok = [r for r in results if r[0] == "OK"]
        skip = [r for r in results if r[0] == "SKIP"]
        timeout = [r for r in results if r[0] == "TIMEOUT"]
        err = [r for r in results if r[0] == "ERROR"]

        print("\n=== Summary (validation run) ===")
        print(f"Saved:   {len(ok)}")
        print(f"Skipped: {len(skip)} (already existed)")
        print(f"Timeout: {len(timeout)}")
        print(f"Errors:  {len(err)}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
