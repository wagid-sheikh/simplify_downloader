from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from app.crm_downloader.td_orders_sync.td_api_artifacts import persist_td_compare_artifacts
from app.crm_downloader.td_orders_sync.td_api_compare import compare_canonical_rows_detailed


def test_compare_artifacts_emit_reviewable_diff_files(tmp_path: Path) -> None:
    report = compare_canonical_rows_detailed(
        ui_rows=[
            {"store_code": "a1", "order_number": "1", "order_date": "2026-01-01", "amount": "100", "status": "Delivered"},
            {"store_code": "a1", "order_number": "2", "order_date": "2026-01-01", "amount": "55", "status": "Pending"},
            {"store_code": "a1", "order_number": "3", "order_date": "2026-01-01", "amount": "40", "status": "Delivered"},
        ],
        api_rows=[
            {"store_code": "a1", "order_number": "1", "order_date": "2026-01-01", "amount": "120", "status": "Delivered"},
            {"store_code": "a1", "order_number": "3", "order_date": "2026-01-01", "amount": "40", "status": "Cancelled"},
            {"store_code": "a1", "order_number": "4", "order_date": "2026-01-01", "amount": "10", "status": "Delivered"},
        ],
        key_fields=("store_code", "order_number", "order_date"),
        sample_limit=10,
    )

    result = persist_td_compare_artifacts(
        download_dir=tmp_path,
        store_code="a1",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 31),
        dataset="orders",
        diff_report=report,
        key_fields=("store_code", "order_number", "order_date"),
        row_sample_cap=1,
    )

    assert set(result.artifact_paths) == {
        "orders_compare_summary",
        "orders_missing_in_api",
        "orders_missing_in_ui",
        "orders_value_mismatches",
    }

    summary_path = Path(result.artifact_paths["orders_compare_summary"])
    missing_api_path = Path(result.artifact_paths["orders_missing_in_api"])
    missing_ui_path = Path(result.artifact_paths["orders_missing_in_ui"])
    mismatch_path = Path(result.artifact_paths["orders_value_mismatches"])

    assert summary_path.name == "A1_td_api_20260101_20260131_orders_compare_summary.json"
    assert missing_api_path.name == "A1_td_api_20260101_20260131_orders_missing_in_api.jsonl"
    assert missing_ui_path.name == "A1_td_api_20260101_20260131_orders_missing_in_ui.jsonl"
    assert mismatch_path.name == "A1_td_api_20260101_20260131_orders_value_mismatches.jsonl"

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["row_sample_cap"] == 1
    assert summary_payload["counts"]["missing_in_api"] == 1
    assert summary_payload["counts"]["missing_in_ui"] == 1
    assert summary_payload["counts"]["amount_mismatches"] == 1
    assert summary_payload["counts"]["status_mismatches"] == 1
    assert summary_payload["truncated"] == {
        "missing_in_api": False,
        "missing_in_ui": False,
        "value_mismatches": True,
    }

    missing_api_row = json.loads(missing_api_path.read_text(encoding="utf-8").strip())
    assert missing_api_row["reason_code"] == "key_missing"
    assert missing_api_row["key_fields"]["order_number"] == "2"

    missing_ui_row = json.loads(missing_ui_path.read_text(encoding="utf-8").strip())
    assert missing_ui_row["reason_code"] == "key_missing"
    assert missing_ui_row["key_fields"]["order_number"] == "4"

    mismatch_row = json.loads(mismatch_path.read_text(encoding="utf-8").strip())
    assert mismatch_row["reason_code"] in {"amount_mismatch", "status_mismatch", "amount_and_status_mismatch"}
