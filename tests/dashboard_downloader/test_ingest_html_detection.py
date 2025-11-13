import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simplify_downloader.common.ingest.service import _load_csv_rows, _looks_like_html


def test_looks_like_html_detects_html(tmp_path):
    html_path = tmp_path / "fake.csv"
    html_path.write_text("<!DOCTYPE html><html><body>login</body></html>", encoding="utf-8")

    assert _looks_like_html(html_path) is True
    assert list(_load_csv_rows("missed_leads", html_path)) == []


def test_load_csv_rows_parses_valid_csv(tmp_path):
    csv_path = tmp_path / "real.csv"
    csv_path.write_text(
        "Pickup Row Id,Store Code,Mobile Number\n1001,A001,9876543210\n",
        encoding="utf-8",
    )

    rows = list(_load_csv_rows("missed_leads", csv_path))
    assert rows == [
        {
            "pickup_row_id": 1001,
            "store_code": "A001",
            "mobile_number": "9876543210",
            "pickup_no": None,
            "pickup_created_date": None,
            "pickup_created_time": None,
            "store_name": None,
            "pickup_date": None,
            "pickup_time": None,
            "customer_name": None,
            "special_instruction": None,
            "source": None,
            "final_source": None,
            "customer_type": None,
            "is_order_placed": None,
        }
    ]
