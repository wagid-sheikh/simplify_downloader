from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Page

from .config import (
    STORES,
    PKG_ROOT,
    DATA_DIR,
    FILE_SPECS,
    MERGED_NAMES,
)

def _ensure_profile_dir(store_name: str) -> Path:
    p = PKG_ROOT / "profiles" / store_name
    p.mkdir(parents=True, exist_ok=True)
    return p

def _inject_download_and_wait(page: Page, url: str, timeout_ms: int = 60_000) -> Path | None:
    """
    Triggers a download for a direct CSV endpoint by injecting an <a download> element
    and clicking it (wrapped in expect_download).
    Returns the temp path Playwright uses, or None if it times out.
    """
    try:
        with page.expect_download(timeout=timeout_ms) as dinfo:
            page.evaluate(
                """url => {
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = '';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                }""",
                url,
            )
        download = dinfo.value
        return Path(download.path())
    except PWTimeoutError:
        return None

def _save_download(page: Page, temp_path: Path, final_path: Path) -> None:
    # When we have a temp path, Playwright still wants save_as() to copy to the final.
    # We re-open the last download handle via page.wait_for_event("download") is not needed
    # because we already have it; instead, save by re-triggering evaluate and letting
    # Playwright handle save_as on the last download object we captured.
    # Simpler: just use final_path.parent.mkdir and then call download.save_as() earlier.
    # But here, we will do the save right after expect_download returns.
    final_path.parent.mkdir(parents=True, exist_ok=True)
    # We cannot copy the temp_path directly because Playwright manages it.
    # The right way is to call save_as on the download handle when we catch it.
    # So in this function we won't do anything; kept for structure.
    pass

def _render(template: str, sc: str) -> str:
    return template.format(sc=sc, ymd=datetime.now().strftime("%Y%m%d"))

def _download_one_spec(page: Page, sc: str, spec: Dict) -> Path | None:
    url = _render(spec["url_template"], sc)
    out_name = _render(spec["out_name_template"], sc)
    final_path = DATA_DIR / out_name

    # Fire the download
    try:
        with page.expect_download(timeout=60_000) as dinfo:
            page.evaluate(
                """url => {
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = '';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                }""",
                url,
            )
        download = dinfo.value
        final_path.parent.mkdir(parents=True, exist_ok=True)
        suggested = download.suggested_filename or out_name
        # Prefer our explicit out_name
        download.save_as(str(final_path))
        return final_path
    except PWTimeoutError:
        print(f"  - Timeout while downloading: {spec['key']} ({url})")
        return None

def _merge_bucket(files: List[Path], output: Path) -> None:
    """
    Simple CSV merge: writes header from the first file, then appends rows from all files.
    Skips empty/missing files gracefully.
    """
    if not files:
        print(f"[merge] No files to merge for {output.name}")
        return

    written_header = False
    rows_written = 0

    with output.open("w", newline="", encoding="utf-8") as out_f:
        writer = None

        for f in files:
            if not f or not f.exists() or f.stat().st_size == 0:
                continue

            with f.open("r", newline="", encoding="utf-8", errors="ignore") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue

                if not written_header:
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                    written_header = True

                for row in reader:
                    writer.writerow(row)
                    rows_written += 1

    if rows_written == 0:
        print(f"[merge] Created empty (header-only) file: {output.name}")
    else:
        print(f"[merge] {output.name} — merged {rows_written} rows from {len(files)} file(s)")

def filter_merged_missed_leads(input_path: Path, output_path: Path) -> None:
    """
    Reads merged_missed_leads_YYYYMMDD.csv, removes:
      - rows where is_order_placed == 1 (accepts 1, "1", "1.0", etc.)
      - rows where customer_type == 'Existing' (case-insensitive, trimmed)
    Writes filtered CSV to filtered_merged_missed_leads_YYYYMMDD.csv.
    Gracefully handles missing columns by copying input to output unchanged.
    """
    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"[filter] Input missing or empty, skipping: {input_path.name}")
        return

    with input_path.open("r", newline="", encoding="utf-8", errors="ignore") as fin:
        reader = csv.reader(fin)
        try:
            header = next(reader)
        except StopIteration:
            print(f"[filter] No rows in {input_path.name}")
            return

        # Column indices (fallback to None if not present)
        def idx(col: str) -> int | None:
            try:
                return header.index(col)
            except ValueError:
                return None

        i_is_order_placed = idx("is_order_placed")
        i_customer_type   = idx("customer_type")

        # If required columns aren’t present, just copy input -> output
        if i_is_order_placed is None or i_customer_type is None:
            print(f"[filter] Required columns missing; copying {input_path.name} unchanged.")
            with input_path.open("r", encoding="utf-8", errors="ignore") as src, \
                 output_path.open("w", encoding="utf-8", newline="") as dst:
                dst.write(src.read())
            return

        rows_out = []
        for row in reader:
            # Defensive: pad short rows
            if len(row) <= max(i_is_order_placed, i_customer_type):
                continue

            raw_is_placed = (row[i_is_order_placed] or "").strip().lower()
            raw_cust_type = (row[i_customer_type] or "").strip().lower()

            # Normalize is_order_placed to numeric-ish
            is_placed = raw_is_placed in {"1", "1.0", "true", "yes"}  # treat these as placed
            is_existing = (raw_cust_type == "existing")

            if is_placed:
                continue
            if is_existing:
                continue

            rows_out.append(row)

    with output_path.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(rows_out)

    print(f"[filter] {output_path.name} — kept {len(rows_out)} rows after filtering.")

def run_all_stores() -> None:
    """
    Opens a persistent profile for each store, goes to the store's TMS dashboard,
    downloads all FILE_SPECS where download=True, saves into downloader/data/,
    and finally performs merges per merge_bucket.
    """
    merged_buckets: Dict[str, List[Path]] = {}

    with sync_playwright() as p:
        # NOTE: On macOS we can keep channel="chrome" if you prefer;
        # leaving it off keeps it portable for Linux server (Chromium).
        for store_name, cfg in STORES.items():
            user_dir = _ensure_profile_dir(store_name)
            sc = cfg["store_code"]
            dashboard_url = cfg["dashboard_url"]

            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(user_dir),
                headless=True,
                accept_downloads=True,
                # macOS dev: uncomment next line to force system Chrome
                channel="chrome",
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )

            try:
                page = ctx.new_page()
                # Hit the dashboard directly (cookie/session should be present from first_login)
                page.goto(dashboard_url, wait_until="domcontentloaded")

                print(f"[{store_name}] At dashboard → {dashboard_url}")

                for spec in FILE_SPECS:
                    if not spec.get("download", True):
                        continue

                    saved = _download_one_spec(page, sc, spec)
                    if saved and spec.get("merge_bucket"):
                        merged_buckets.setdefault(spec["merge_bucket"], []).append(saved)

                print(f"[{store_name}] Done.")

            finally:
                ctx.close()

    # ---- Merges (by bucket) ----
    for bucket, files in merged_buckets.items():
        out_name = MERGED_NAMES.get(bucket, f"Merged_{bucket}_{datetime.now().strftime('%Y%m%d')}.csv")
        out_path = DATA_DIR / out_name
        _merge_bucket(files, out_path)

        # Auto-filter for missed_leads bucket
        if bucket == "missed_leads":
            filtered_name = f"filtered_merged_missed_leads_{datetime.now().strftime('%Y%m%d')}.csv"
            filtered_path = DATA_DIR / filtered_name
            filter_merged_missed_leads(out_path, filtered_path)

if __name__ == "__main__":
    run_all_stores()

