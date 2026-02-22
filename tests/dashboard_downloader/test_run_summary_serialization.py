from __future__ import annotations

from decimal import Decimal

from app.dashboard_downloader.run_summary import _normalize_json_for_db


def test_normalize_json_for_db_converts_nested_decimals() -> None:
    payload = {
        "metrics": {
            "amount": Decimal("12.34"),
            "stores": [
                {"code": "A1", "totals": {"tax": Decimal("1.23")}},
                Decimal("0.5"),
            ],
        }
    }

    normalized = _normalize_json_for_db(payload)

    assert normalized["metrics"]["amount"] == 12.34
    assert normalized["metrics"]["stores"][0]["totals"]["tax"] == 1.23
    assert normalized["metrics"]["stores"][1] == 0.5
