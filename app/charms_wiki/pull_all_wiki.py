import asyncio
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


START_URL = "https://charmswiki.com/pages/allpages"

# Save under the code file folder: ./Wiki-PDF/
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "Wiki-PDF"

# CSV is also saved next to the script
CSV_PATH = BASE_DIR / "wiki_links.csv"

NAV_TIMEOUT_MS = 45_000
RETRY_COUNT = 2

# Validation run: download only 2
MAX_TO_SAVE = 2


@dataclass
class LinkRow:
    seq: int
    name: str
    url: str


def chrome_executable_path() -> str:
    """
    Prefer explicit override, else use standard macOS Chrome path.
    Set CHROME_PATH if needed.
    """
    import os
    env = os.environ.get("CHROME_PATH")
    if env and Path(env).exists():
        return env

    candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for p in candidates:
        if Path(p).exists():
            return p

    raise FileNotFoundError(
        "Could not find a local Chrome/Canary/Chromium executable. "
        "Install Google Chrome, or set CHROME_PATH to the full executable path."
    )


def is_same_site(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


def sanitize_filename(name: str, max_len: int = 160) -> str:
    """
    Sanitize for cross-platform safety while keeping it readable.
    """
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r'[\/\\\:\*\?\"\<\>\|\n\r\t]', "-", name)
    name = re.sub(r"[\u0000-\u001f]", "", name)
    name = name.strip(" .-_")
    if not name:
        name = "untitled"
    if len(name) > max_len:
        name = name[:max_len].rstrip(" .-_")
    return name


def unique_path_for_label(out_dir: Path, label: str) -> Path:
    """
    Ensure file path uniqueness if multiple links share the same label.
    """
    base = sanitize_filename(label)
    candidate = out_dir / f"{base}.pdf"
    if not candidate.exists():
        return candidate

    for i in range(2, 10_000):
        candidate = out_dir / f"{base} ({i}).pdf"
        if not candidate.exists():
            return candidate

    return out_dir / f"{base} (duplicate).pdf"


async def collect_link_pairs(page) -> list[tuple[str, str]]:
    """
    Collect (label_text, absolute_url) from the All Pages index.
    Label is the anchor text visible to the user.
    """
    await page.goto(START_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_timeout(1000)

    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            href: e.getAttribute('href'),
            text: (e.textContent || '').trim()
        }))"""
    )

    pairs: list[tuple[str, str]] = []
    for a in anchors:
        href = (a.get("href") or "").strip()
        text = (a.get("text") or "").strip()

        if not href:
            continue
        if href.startswith("#") or href.lower().startswith(("javascript:", "mailto:")):
            continue
        if not text:
            continue

        u = urljoin(START_URL, href)
        if not u.startswith(("http://", "https://")):
            continue
        if not is_same_site(u, START_URL):
            continue
        if u.rstrip("/") == START_URL.rstrip("/"):
            continue

        pairs.append((text, u))

    # Deduplicate by URL (keep first label)
    seen_urls = set()
    deduped: list[tuple[str, str]] = []
    for label, u in pairs:
        if u not in seen_urls:
            seen_urls.add(u)
            deduped.append((label, u))

    return deduped


def write_links_csv(pairs: list[tuple[str, str]], csv_path: Path) -> None:
    """
    Write CSV with columns: Sequence, Link Name, URL
    """
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Sequence", "Link Name", "URL"])
        for i, (name, url) in enumerate(pairs, start=1):
            w.writerow([i, name, url])


def read_links_csv(csv_path: Path) -> list[LinkRow]:
    """
    Read CSV back into LinkRow list.
    """
    rows: list[LinkRow] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        expected = {"Sequence", "Link Name", "URL"}
        if set(r.fieldnames or []) != expected:
            raise ValueError(
                f"CSV header mismatch. Expected {expected}, got {set(r.fieldnames or [])}"
            )
        for line in r:
            seq = int(line["Sequence"])
            name = (line["Link Name"] or "").strip()
            url = (line["URL"] or "").strip()
            if name and url:
                rows.append(LinkRow(seq=seq, name=name, url=url))
    return rows


async def save_page_pdf(context, label: str, url: str, idx: int, total: int):
    for attempt in range(RETRY_COUNT + 1):
        page = await context.new_page()
        try:
            print(f"[{idx}/{total}] Saving: {label}")
            print(f"[{idx}/{total}] URL: {url}")

            await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)

            pdf_path = unique_path_for_label(OUT_DIR, label)

            if pdf_path.exists():
                print(f"[{idx}/{total}] SKIP (exists): {pdf_path.relative_to(BASE_DIR)}\n")
                await page.close()
                return ("SKIP", url, str(pdf_path))

            await page.pdf(
                path=str(pdf_path),
                format="A4",
                print_background=True,
                margin={"top": "12mm", "right": "12mm", "bottom": "12mm", "left": "12mm"},
                prefer_css_page_size=True,
            )

            print(f"[{idx}/{total}] OK   Saved: {pdf_path.relative_to(BASE_DIR)}\n")
            await page.close()
            return ("OK", url, str(pdf_path))

        except PlaywrightTimeoutError:
            await page.close()
            if attempt >= RETRY_COUNT:
                print(f"[{idx}/{total}] TIMEOUT: {url}\n")
                return ("TIMEOUT", url, "")
            print(f"[{idx}/{total}] Timeout, retrying ({attempt+1}/{RETRY_COUNT})...\n")
            await asyncio.sleep(1.5)

        except Exception as e:
            await page.close()
            if attempt >= RETRY_COUNT:
                print(f"[{idx}/{total}] ERROR: {url} => {type(e).__name__}: {e}\n")
                return ("ERROR", url, f"{type(e).__name__}: {e}")
            print(f"[{idx}/{total}] Error, retrying ({attempt+1}/{RETRY_COUNT})... => {type(e).__name__}: {e}\n")
            await asyncio.sleep(1.5)


async def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"PDF output directory: {OUT_DIR} (relative: {OUT_DIR.relative_to(BASE_DIR)})")
    print(f"CSV output path:      {CSV_PATH} (relative: {CSV_PATH.relative_to(BASE_DIR)})")

    chrome_path = chrome_executable_path()
    print(f"Using local Chrome executable: {chrome_path}\n")

    # 1) Scrape links from index and write CSV first
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
        pairs = await collect_link_pairs(index_page)
        await index_page.close()

        print(f"Found {len(pairs)} links on index page.")
        write_links_csv(pairs, CSV_PATH)
        print(f"Saved link list CSV: {CSV_PATH}\n")

        # 2) Read CSV back and use it as the download queue
        link_rows = read_links_csv(CSV_PATH)

        total_found = len(link_rows)
        to_save = min(MAX_TO_SAVE, total_found)

        print(f"CSV contains {total_found} links.")
        print(f"Will download {to_save} PDFs (validation run).\n")

        results = []
        for i, row in enumerate(link_rows[:to_save], start=1):
            # Progress is relative to the validation batch size (to_save)
            res = await save_page_pdf(context, row.name, row.url, i, to_save)
            results.append(res)

        await context.close()
        await browser.close()

        ok = [r for r in results if r[0] == "OK"]
        skip = [r for r in results if r[0] == "SKIP"]
        timeout = [r for r in results if r[0] == "TIMEOUT"]
        err = [r for r in results if r[0] == "ERROR"]

        print("=== Summary (validation run) ===")
        print(f"Saved:   {len(ok)}")
        print(f"Skipped: {len(skip)}")
        print(f"Timeout: {len(timeout)}")
        print(f"Errors:  {len(err)}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
