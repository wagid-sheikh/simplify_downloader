#!/usr/bin/env python3
"""
Store List PDF Parser – Geometry-Based, Production Grade

Reads:
    store-list-tsm-wise.pdf   (same folder as this script)

Writes:
    store-list-clean.csv
    store-list-rejected.csv

Clean CSV schema:
    launch_date, store_code, store_name, total_order, page, row_index

Rejected CSV schema:
    raw_line, store_code, rejection_reason, page, row_index
"""

from __future__ import annotations

import csv
import datetime
import re
from pathlib import Path

import pdfplumber


# ------------------- helpers -------------------


DATE_RE = re.compile(r"\b\d{2}-\d{2}-\d{4}\b")
CODE_RE = re.compile(r"^[A-Z]{1,3}\d{1,4}$")


def normalize(text: str | None) -> str:
    """Collapse newlines/tabs/multiple spaces into a single space."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def parse_date_to_iso(text: str) -> str:
    """Pick first dd-mm-yyyy in text and convert to yyyy-mm-dd. On failure, return ""."""
    txt = normalize(text)
    m = DATE_RE.search(txt)
    if not m:
        return ""
    token = m.group(0)
    try:
        dt = datetime.datetime.strptime(token, "%d-%m-%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def parse_total_order(text: str):
    """
    Extract numeric TotalOrder from the cell.
    Keep only digits; if no digits, return "".
    """
    txt = normalize(text)
    digits = re.sub(r"[^\d]", "", txt)
    if not digits:
        return ""
    return int(digits)


def group_rows(words, y_tol: float = 2.5):
    """
    Group words into rows based on their 'top' coordinate.
    All words whose 'top' differ by <= y_tol are treated as the same row.
    """
    import statistics

    rows: list[list[dict]] = []
    for w in sorted(words, key=lambda w: w["top"]):
        if not rows:
            rows.append([w])
            continue
        last_top = statistics.mean(x["top"] for x in rows[-1])
        if abs(w["top"] - last_top) <= y_tol:
            rows[-1].append(w)
        else:
            rows.append([w])
    return rows


# ------------------- column discovery from header -------------------


def discover_column_positions(page) -> dict:
    """
    Look at the header row on the first page and infer approximate X positions for:
        - launch
        - code
        - store_name
        - total_order1 (first TotalOrder column)

    Returns a dict with keys: x_launch, x_code, x_store_name, x_tot1
    """
    words = page.extract_words()
    # Limit to header region (top ~ 80–120)
    header_words = [w for w in words if 80 <= w["top"] <= 120]

    # Launch
    x_launch = next(w["x0"] for w in header_words if w["text"] == "Launch")

    # Code column: "Code" is on the second header line
    x_code = next(w["x0"] for w in header_words if w["text"] == "Code")

    # Store name: there are two "Store" words, one for Store Code and one for Store Name.
    # We take the one with larger x0 as Store Name.
    store_words = [w for w in header_words if w["text"] == "Store"]
    if len(store_words) >= 2:
        x_store_name = max(store_words, key=lambda w: w["x0"])["x0"]
    else:
        # Fallback: assume store name is to the right of code by a bit
        x_store_name = x_code + 40

    # TotalOrder: there are two TotalOrder columns; pick the leftmost as the first TotalOrder
    total_words = [w for w in header_words if w["text"] == "TotalOrder"]
    total_words = sorted(total_words, key=lambda w: w["x0"])
    if not total_words:
        raise RuntimeError("Could not find TotalOrder header in PDF.")
    x_tot1 = total_words[0]["x0"]

    return {
        "x_launch": x_launch,
        "x_code": x_code,
        "x_store_name": x_store_name,
        "x_tot1": x_tot1,
    }


# ------------------- core extraction per page -------------------


def parse_page_stores(page, col_pos: dict, page_number: int):
    """
    Extract store rows from a single page using word positions.

    For each row:
      - detect a store code (A814, TS0, C021, ...)
      - find launch date near launch column
      - assemble store name from the store-name column words
      - find total_order from the first TotalOrder column
    """
    words = page.extract_words()
    rows = group_rows(words)

    stores = []
    rejected = []

    x_launch = col_pos["x_launch"]
    x_code = col_pos["x_code"]
    x_store_name = col_pos["x_store_name"]
    x_tot1 = col_pos["x_tot1"]

    # boundaries for store-name region, roughly between code and TotalOrder
    left_name = (x_code + x_store_name) / 2.0
    right_name = (x_store_name + x_tot1) / 2.0

    for r_idx, row in enumerate(rows):
        # 1) find store code in row (any word matching CODE_RE)
        code_word = None
        for w in row:
            if CODE_RE.match(w["text"]):
                code_word = w
                break

        if not code_word:
            continue  # not a store row

        # we keep the raw line for diagnostics
        raw_line = " ".join(w["text"] for w in sorted(row, key=lambda w: w["x0"]))

        # 2) launch date: date-like word near the launch column
        launch_word = None
        for w in row:
            if DATE_RE.match(w["text"]) and abs(w["x0"] - x_launch) < 40:
                launch_word = w
                break

        launch_date_iso = parse_date_to_iso(launch_word["text"]) if launch_word else ""

        # 3) store name: words whose x0 fall between left_name and right_name
        name_words = [
            w for w in row if left_name <= w["x0"] <= right_name
        ]
        store_name = " ".join(w["text"] for w in sorted(name_words, key=lambda w: w["x0"]))

        # 4) total_order: first integer-like word near the first TotalOrder column
        total_word = None
        for w in sorted(row, key=lambda w: w["x0"]):
            if abs(w["x0"] - x_tot1) < 30 and re.fullmatch(r"\d+", w["text"]):
                total_word = w
                break

        total_order = parse_total_order(total_word["text"]) if total_word else ""

        # Basic validation: we must have a store_code and at least some store_name text
        store_code = code_word["text"]

        if not store_name:
            rejected.append(
                {
                    "raw_line": raw_line,
                    "store_code": store_code,
                    "rejection_reason": "MISSING_STORE_NAME",
                    "page": page_number,
                    "row_index": r_idx,
                }
            )
            continue

        stores.append(
            {
                "launch_date": launch_date_iso,
                "store_code": store_code,
                "store_name": store_name,
                "total_order": total_order,
                "page": page_number,
                "row_index": r_idx,
            }
        )

    return stores, rejected


# ------------------- main extraction -------------------


def extract_all_stores(pdf_path: Path):
    """
    Drive the full PDF parsing:
        - discover column positions from page 1
        - extract stores from all pages
    """
    clean_rows: list[dict] = []
    rejected_rows: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            raise RuntimeError("PDF has no pages.")

        # discover positions once from the first page
        col_pos = discover_column_positions(pdf.pages[0])

        for i, page in enumerate(pdf.pages, start=1):
            stores, rejected = parse_page_stores(page, col_pos, page_number=i)
            clean_rows.extend(stores)
            rejected_rows.extend(rejected)

    return clean_rows, rejected_rows


# ------------------- CSV writers -------------------


def write_clean_csv(clean_rows: list[dict], path: Path) -> None:
    fields = ["launch_date", "store_code", "store_name", "total_order", "page", "row_index"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in clean_rows:
            writer.writerow(r)


def write_rejected_csv(rejected_rows: list[dict], path: Path) -> None:
    fields = ["raw_line", "store_code", "rejection_reason", "page", "row_index"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rejected_rows:
            writer.writerow(r)


# ------------------- main entrypoint -------------------


def main() -> None:
    base_dir = Path(__file__).resolve().parent

    pdf_path = base_dir / "store-list-tsm-wise.pdf"
    clean_path = base_dir / "store-list-clean.csv"
    rejected_path = base_dir / "store-list-rejected.csv"

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found at: {pdf_path}")

    clean_rows, rejected_rows = extract_all_stores(pdf_path)

    write_clean_csv(clean_rows, clean_path)
    write_rejected_csv(rejected_rows, rejected_path)

    print(f"Clean rows:    {len(clean_rows)}")
    print(f"Rejected rows: {len(rejected_rows)}")
    print(f"Clean CSV:     {clean_path}")
    print(f"Rejected CSV:  {rejected_path}")


if __name__ == "__main__":
    main()
