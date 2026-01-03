#!/usr/bin/env python3
"""
CharmsWiki Reference Binder Builder (PDF-only)

- Reads existing wiki_links_grouped.csv (authoritative order) from the same folder as this script.
- Uses local Google Chrome (not Playwright bundled Chromium) for compatibility.
- Caches each visited page as an individual PDF (idempotent).
- Maintains resume state (merge_state.json) so runs can continue after interruptions.
- Produces:
  1) CharmsWiki_REFERENCE_MERGED.pdf  (content-only, incremental merge)
  2) CharmsWiki_REFERENCE_FINAL.pdf   (Intro + Table of Contents prepended)

Table of Contents is generated AFTER download/merge, then inserted at the front of FINAL.
"""

import asyncio
import csv
import json
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pypdf import PdfReader, PdfWriter

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


# ---------- Inputs (existing files; NOT regenerated) ----------
BASE_DIR = Path(__file__).resolve().parent
GROUPED_CSV = BASE_DIR / "wiki_links_grouped.csv"
RAW_CSV = BASE_DIR / "wiki_links.csv"  # present per your process; not used except presence check

# ---------- Output ----------
OUT_DIR = BASE_DIR / "Wiki-PDF"
CACHE_DIR = OUT_DIR / "_cache_pages_pdf"

STATE_PATH = OUT_DIR / "merge_state.json"

MERGED_PDF = OUT_DIR / "CharmsWiki_REFERENCE_MERGED.pdf"
MERGED_TMP = OUT_DIR / "CharmsWiki_REFERENCE_MERGED.tmp.pdf"

FRONTMATTER_PDF = OUT_DIR / "_frontmatter_intro_toc.pdf"
FINAL_PDF = OUT_DIR / "CharmsWiki_REFERENCE_FINAL.pdf"
FINAL_TMP = OUT_DIR / "CharmsWiki_REFERENCE_FINAL.tmp.pdf"

# ---------- Controls ----------
NAV_TIMEOUT_MS = 45_000
RETRY_COUNT = 2

# Validation run: keep 2; set to None for full run (388+)
MAX_TO_SAVE: Optional[int] = None

# Throttle delay to reduce rate limiting
DELAY_MIN = 15
DELAY_MAX = 20


@dataclass(frozen=True)
class LinkRow:
    design_seq: int
    seq: int
    group: str
    subgroup: str
    name: str
    url: str


def chrome_executable_path() -> str:
    """
    Uses CHROME_PATH if set; else typical macOS paths.
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
        "Local Chrome executable not found. Install Google Chrome or set CHROME_PATH."
    )


def sanitize_filename(name: str, max_len: int = 140) -> str:
    """
    Safe filename derived from Link Name (human readable).
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
    safe = sanitize_filename(label, max_len=120)
    return CACHE_DIR / f"{design_seq:04d} - {safe}.pdf"


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"last_completed_design_seq": 0, "merged_design_seqs": []}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def read_grouped_csv(path: Path) -> List[LinkRow]:
    """
    Expected header:
    Design Sequence,Sequence,Group,Subgroup,Link Name,URL
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")

    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"Design Sequence", "Sequence", "Group", "Subgroup", "Link Name", "URL"}
        got = set(r.fieldnames or [])
        if required != got:
            raise ValueError(
                f"Grouped CSV header mismatch.\nExpected: {required}\nGot:      {got}\nFile: {path}"
            )

        rows: List[LinkRow] = []
        for line in r:
            name = (line["Link Name"] or "").strip()
            url = (line["URL"] or "").strip()
            if not name or not url:
                continue
            rows.append(
                LinkRow(
                    design_seq=int(line["Design Sequence"]),
                    seq=int(line["Sequence"]),
                    group=(line["Group"] or "").strip(),
                    subgroup=(line["Subgroup"] or "").strip(),
                    name=name,
                    url=url,
                )
            )

    rows.sort(key=lambda x: x.design_seq)
    return rows


async def download_page_pdf(context, row: LinkRow, idx: int, total: int) -> Path:
    """
    Downloads a single page as a PDF (rendered) into CACHE_DIR.
    Idempotent: returns existing cache if present.
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

    # Should never reach here
    return pdf_path


def append_to_merged(merged_path: Path, new_pdf: Path) -> None:
    """
    Incrementally appends a PDF onto merged_path. merged_path remains valid after each call.
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


def rebuild_merged_from_cache(rows: List[LinkRow]) -> None:
    """
    Self-heal: rebuild MERGED_PDF from cached PDFs in design order.
    Useful when state indicates completion but merged file is missing/corrupt.
    """
    writer = PdfWriter()
    added_any = False

    for row in rows:
        p = cache_pdf_path(row.design_seq, row.name)
        if not p.exists():
            continue
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)
        added_any = True

    if not added_any:
        return

    with MERGED_TMP.open("wb") as f:
        writer.write(f)

    MERGED_TMP.replace(MERGED_PDF)


def compute_start_pages(rows: List[LinkRow], frontmatter_pages: int = 0) -> Dict[int, int]:
    """
    Starting page number (1-based) for each design_seq within the FINAL PDF.
    frontmatter_pages is added as an offset because intro+TOC are prepended.
    """
    starts: Dict[int, int] = {}
    cursor = 1 + int(frontmatter_pages)

    for row in rows:
        p = cache_pdf_path(row.design_seq, row.name)
        if not p.exists():
            continue
        starts[row.design_seq] = cursor
        reader = PdfReader(str(p))
        cursor += len(reader.pages)

    return starts


def _draw_wrapped_text(
    c: canvas.Canvas,
    text: str,
    x: float,
    y: float,
    max_width: float,
    line_height: float,
) -> float:
    """
    Minimal wrapping for ReportLab. Returns updated y.
    """
    words = text.split()
    line: List[str] = []

    while words:
        line.append(words.pop(0))
        w = c.stringWidth(" ".join(line), "Helvetica", 11)
        if w > max_width:
            last = line.pop()
            c.drawString(x, y, " ".join(line))
            y -= line_height
            line = [last]
            if y < 60:
                c.showPage()
                y = A4[1] - 60
                c.setFont("Helvetica", 11)

    if line:
        c.drawString(x, y, " ".join(line))
        y -= line_height

    return y


def generate_frontmatter_pdf(rows: List[LinkRow], starts: Dict[int, int], out_pdf: Path) -> int:
    """
    Creates Intro + TOC PDF. Returns number of pages in the created PDF.
    """
    c = canvas.Canvas(str(out_pdf), pagesize=A4)
    width, height = A4

    # --- Intro ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 70, "CharmsWiki Reference Binder (Grouped)")

    c.setFont("Helvetica", 11)
    intro = (
        "This PDF is an automatically compiled reference binder created by visiting CharmsWiki pages in a "
        "system-design-oriented order (grouped by modules such as Platform & Access, Case Management, "
        "Carer Lifecycle, Placements, Finance, Reporting, and Documents). "
        "Each section in this binder is the original page as rendered in a browser (including images, tables, and layout). "
        "Use the Table of Contents to jump to the starting page of any reference item. "
        "The ordering is controlled by wiki_links_grouped.csv."
    )
    y = height - 110
    y = _draw_wrapped_text(c, intro, 50, y, max_width=width - 100, line_height=16)

    c.setFont("Helvetica", 10)
    c.drawString(50, 70, "Tip: If you regenerate this binder later, page numbers may change; regenerate the TOC as well.")
    c.showPage()

    # --- TOC ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 70, "Table of Contents")
    c.setFont("Helvetica", 10)
    c.drawString(50, height - 90, "Page numbers refer to CharmsWiki_REFERENCE_FINAL.pdf (this file).")

    y = height - 125
    last_group = None
    last_sub = None

    def ensure_space(lines: int = 1):
        nonlocal y
        if y - (lines * 16) < 60:
            c.showPage()
            c.setFont("Helvetica", 11)
            y = height - 60

    for row in rows:
        page_no = starts.get(row.design_seq)
        if not page_no:
            continue

        if row.group != last_group:
            ensure_space(2)
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, y, row.group)
            y -= 18
            last_group = row.group
            last_sub = None

        if row.subgroup != last_sub:
            ensure_space(2)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(70, y, row.subgroup)
            y -= 16
            last_sub = row.subgroup

        ensure_space(1)
        c.setFont("Helvetica", 10)

        title = f"{row.design_seq:04d}. {row.name}"
        if len(title) > 110:
            title = title[:107] + "..."

        c.drawString(90, y, title)
        c.drawRightString(width - 50, y, str(page_no))
        y -= 14

    c.save()

    fm_reader = PdfReader(str(out_pdf))
    return len(fm_reader.pages)


def prepend_frontmatter(frontmatter_pdf: Path, merged_pdf: Path, final_pdf: Path) -> None:
    """
    final_pdf = frontmatter + merged
    """
    writer = PdfWriter()

    fm = PdfReader(str(frontmatter_pdf))
    for p in fm.pages:
        writer.add_page(p)

    if merged_pdf.exists():
        merged = PdfReader(str(merged_pdf))
        for p in merged.pages:
            writer.add_page(p)

    with FINAL_TMP.open("wb") as f:
        writer.write(f)

    FINAL_TMP.replace(final_pdf)


async def main() -> None:
    # Hard requirement: read existing files, do not regenerate
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Expected existing file not found: {RAW_CSV}")
    if not GROUPED_CSV.exists():
        raise FileNotFoundError(f"Expected existing file not found: {GROUPED_CSV}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    rows = read_grouped_csv(GROUPED_CSV)

    if MAX_TO_SAVE is not None:
        rows = rows[:MAX_TO_SAVE]

    total = len(rows)
    if total == 0:
        raise RuntimeError("No rows found in wiki_links_grouped.csv (after filtering).")

    state = load_state()
    last_completed = int(state.get("last_completed_design_seq", 0))
    merged_set: Set[int] = set(state.get("merged_design_seqs", []))

    print(f"Script dir:      {BASE_DIR}")
    print(f"Grouped CSV:     {GROUPED_CSV.name}")
    print(f"Cache dir:       {CACHE_DIR}")
    print(f"Merged PDF:      {MERGED_PDF.name}")
    print(f"Final PDF:       {FINAL_PDF.name}")
    print(f"State file:      {STATE_PATH.name}")
    print(f"Total (this run):{total}")
    print(f"Resume: last_completed_design_seq={last_completed}, merged={len(merged_set)}\n")

    chrome_path = chrome_executable_path()
    print(f"Using local Chrome: {chrome_path}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            executable_path=chrome_path,
            args=["--no-first-run", "--no-default-browser-check", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()

        for idx, row in enumerate(rows, start=1):
            cached_pdf = cache_pdf_path(row.design_seq, row.name)

            # Self-healing skip rule:
            # - Only skip if state says done AND the cache file exists.
            # - If cache is missing, we must fetch again even if state says done.
            if row.design_seq <= last_completed and cached_pdf.exists():
                print(f"[{idx}/{total}] SKIP DONE  {row.design_seq:04d} :: {row.name}")
                continue

            pdf_path = await download_page_pdf(context, row, idx, total)

            # Merge rule:
            # - If state says it's already merged, we do not append again.
            # - BUT if MERGED_PDF is missing, we'll rebuild later from cache.
            if row.design_seq not in merged_set:
                print(f"[{idx}/{total}] MERGE      + {pdf_path.name}")
                append_to_merged(MERGED_PDF, pdf_path)
                merged_set.add(row.design_seq)

            # checkpoint after successful fetch (+ merge attempt)
            state["last_completed_design_seq"] = row.design_seq
            state["merged_design_seqs"] = sorted(merged_set)
            save_state(state)

            delay = random.randint(DELAY_MIN, DELAY_MAX)
            print(f"[{idx}/{total}] WAIT       {delay}s\n")
            await asyncio.sleep(delay)

        await context.close()
        await browser.close()

    # Self-heal merged PDF if missing
    if not MERGED_PDF.exists():
        print("Merged PDF missing. Rebuilding from cached PDFs...")
        rebuild_merged_from_cache(rows)

    # If still missing, we can't make a final binder
    if not MERGED_PDF.exists():
        raise RuntimeError(
            "Merged PDF was not created and could not be rebuilt. "
            "Verify that cached per-page PDFs exist in Wiki-PDF/_cache_pages_pdf/."
        )

    # Two-pass frontmatter:
    # Pass 1: build frontmatter with temporary page numbers (no offset)
    print("Generating Intro + TOC (pass 1)...")
    starts_pass1 = compute_start_pages(rows, frontmatter_pages=0)
    fm_pages = generate_frontmatter_pdf(rows, starts_pass1, FRONTMATTER_PDF)

    # Pass 2: rebuild TOC with correct offset
    print("Generating Intro + TOC (pass 2 with correct offsets)...")
    starts_pass2 = compute_start_pages(rows, frontmatter_pages=fm_pages)
    generate_frontmatter_pdf(rows, starts_pass2, FRONTMATTER_PDF)

    print("Prepending Intro + TOC to merged binder...")
    prepend_frontmatter(FRONTMATTER_PDF, MERGED_PDF, FINAL_PDF)

    # Sanity check: final should have more pages than frontmatter alone
    final_pages = len(PdfReader(str(FINAL_PDF)).pages)
    fm_pages_check = len(PdfReader(str(FRONTMATTER_PDF)).pages)
    if final_pages <= fm_pages_check:
        raise RuntimeError(
            "Final PDF contains only frontmatter (Intro/TOC) and no merged content. "
            "This indicates cached PDFs were not merged or MERGED_PDF is empty."
        )

    print("\nDone.")
    print(f"Cache:      {CACHE_DIR}")
    print(f"Merged PDF: {MERGED_PDF}")
    print(f"Final PDF:  {FINAL_PDF}")
    print(f"State:      {STATE_PATH}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
