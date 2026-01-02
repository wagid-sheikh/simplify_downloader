import asyncio
import csv
import json
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pypdf import PdfReader, PdfWriter


# ---------- Paths (all relative to this script file) ----------
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "Wiki-PDF"
CACHE_DIR = OUT_DIR / "_cache_pages"

RAW_CSV = BASE_DIR / "wiki_links.csv"
GROUPED_CSV = BASE_DIR / "wiki_links_grouped.csv"

STATE_PATH = OUT_DIR / "merge_state.json"
MERGED_PDF = OUT_DIR / "CharmsWiki_REFERENCE_MERGED.pdf"
MERGED_TMP = OUT_DIR / "CharmsWiki_REFERENCE_MERGED.tmp.pdf"


# ---------- Controls ----------
NAV_TIMEOUT_MS = 45_000
RETRY_COUNT = 2

# Keep 2 for validation, then set to None for full run (388+)
MAX_TO_SAVE = 2

# Throttle delay between pages
DELAY_MIN = 15
DELAY_MAX = 20


@dataclass
class LinkRow:
    design_seq: int
    seq: int
    group: str
    subgroup: str
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


def sanitize_filename(name: str, max_len: int = 140) -> str:
    """
    Safe filename derived from Link Name (human-readable anchor text).
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


def cache_pdf_path(design_seq: int, label: str) -> Path:
    """
    Stable, ordered cache file name: "0001 - <Label>.pdf"
    """
    safe = sanitize_filename(label)
    return CACHE_DIR / f"{design_seq:04d} - {safe}.pdf"


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"last_completed_design_seq": 0, "merged_design_seqs": []}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def read_grouped_csv(path: Path) -> list[LinkRow]:
    """
    Expected header (as produced earlier):
    Design Sequence,Sequence,Group,Subgroup,Link Name,URL
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")

    rows: list[LinkRow] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)

        required = {"Design Sequence", "Sequence", "Group", "Subgroup", "Link Name", "URL"}
        got = set(r.fieldnames or [])
        if required != got:
            raise ValueError(
                f"Grouped CSV header mismatch.\nExpected: {required}\nGot:      {got}\nFile: {path}"
            )

        for line in r:
            rows.append(
                LinkRow(
                    design_seq=int(line["Design Sequence"]),
                    seq=int(line["Sequence"]),
                    group=(line["Group"] or "").strip(),
                    subgroup=(line["Subgroup"] or "").strip(),
                    name=(line["Link Name"] or "").strip(),
                    url=(line["URL"] or "").strip(),
                )
            )

    rows.sort(key=lambda x: x.design_seq)
    return [x for x in rows if x.name and x.url]


async def download_page_pdf(context, row: LinkRow, idx: int, total: int) -> Path:
    """
    Downloads a single page as PDF into CACHE_DIR.
    Idempotent: if cached PDF exists, it is reused.
    """
    pdf_path = cache_pdf_path(row.design_seq, row.name)
    if pdf_path.exists():
        print(f"[{idx}/{total}] CACHE HIT  {pdf_path.name}")
        return pdf_path

    for attempt in range(RETRY_COUNT + 1):
        page = await context.new_page()
        try:
            print(f"[{idx}/{total}] FETCH      {row.group} > {row.subgroup} :: {row.name}")
            await page.goto(row.url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)

            await page.pdf(
                path=str(pdf_path),
                format="A4",
                print_background=True,
                margin={"top": "12mm", "right": "12mm", "bottom": "12mm", "left": "12mm"},
                prefer_css_page_size=True,
            )

            await page.close()
            return pdf_path

        except PlaywrightTimeoutError:
            await page.close()
            if attempt >= RETRY_COUNT:
                raise
            print(f"[{idx}/{total}] Timeout, retrying ({attempt+1}/{RETRY_COUNT})...")
            await asyncio.sleep(1.5)

        except Exception:
            await page.close()
            if attempt >= RETRY_COUNT:
                raise
            print(f"[{idx}/{total}] Error, retrying ({attempt+1}/{RETRY_COUNT})...")
            await asyncio.sleep(1.5)


def append_to_merged(merged_path: Path, new_pdf: Path) -> None:
    """
    Idempotent append:
    - Reads existing merged PDF (if any)
    - Appends new_pdf pages
    - Writes to temp, replaces final

    Ensures MERGED_PDF remains valid after each step.
    """
    writer = PdfWriter()

    if merged_path.exists():
        existing = PdfReader(str(merged_path))
        for p in existing.pages:
            writer.add_page(p)

    incoming = PdfReader(str(new_pdf))
    for p in incoming.pages:
        writer.add_page(p)

    with MERGED_TMP.open("wb") as f:
        writer.write(f)

    MERGED_TMP.replace(merged_path)


async def main():
    # Ensure required CSVs exist (per your requirement: read existing files)
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Expected existing file not found: {RAW_CSV}")
    if not GROUPED_CSV.exists():
        raise FileNotFoundError(f"Expected existing file not found: {GROUPED_CSV}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Script directory: {BASE_DIR}")
    print(f"Using RAW CSV:    {RAW_CSV.name}")
    print(f"Using GROUPED:    {GROUPED_CSV.name}")
    print(f"Output folder:    {OUT_DIR}")
    print(f"Cache folder:     {CACHE_DIR}")
    print(f"Merged PDF:       {MERGED_PDF.name}")
    print(f"State file:       {STATE_PATH.name}\n")

    rows = read_grouped_csv(GROUPED_CSV)

    # Validation cap
    if MAX_TO_SAVE is not None:
        rows = rows[:MAX_TO_SAVE]

    total = len(rows)
    print(f"Total rows to process now: {total}")

    state = load_state()
    last_completed = int(state.get("last_completed_design_seq", 0))
    merged_set = set(state.get("merged_design_seqs", []))
    print(f"Resume state: last_completed_design_seq={last_completed}, merged={len(merged_set)}\n")

    chrome_path = chrome_executable_path()
    print(f"Using local Chrome executable: {chrome_path}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            executable_path=chrome_path,
            args=["--no-first-run", "--no-default-browser-check", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()

        for idx, row in enumerate(rows, start=1):
            if row.design_seq <= last_completed:
                print(f"[{idx}/{total}] SKIP DONE  {row.design_seq:04d} :: {row.name}")
                continue

            pdf_path = await download_page_pdf(context, row, idx, total)

            if row.design_seq not in merged_set:
                print(f"[{idx}/{total}] MERGE      + {pdf_path.name}")
                append_to_merged(MERGED_PDF, pdf_path)
                merged_set.add(row.design_seq)

            # checkpoint after successful fetch+merge
            state["last_completed_design_seq"] = row.design_seq
            state["merged_design_seqs"] = sorted(merged_set)
            save_state(state)

            delay = random.randint(DELAY_MIN, DELAY_MAX)
            print(f"[{idx}/{total}] WAIT       {delay}s\n")
            await asyncio.sleep(delay)

        await context.close()
        await browser.close()

    print("Done.")
    print(f"Merged PDF: {MERGED_PDF}")
    print(f"State:      {STATE_PATH}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
