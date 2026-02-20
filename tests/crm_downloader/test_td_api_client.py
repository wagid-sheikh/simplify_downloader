from __future__ import annotations

import json
from pathlib import Path

from app.crm_downloader.td_orders_sync.main import _build_parser
from app.crm_downloader.td_orders_sync.td_api_client import TdApiClient, _extract_rows, _normalize_garment_rows


def test_source_mode_parser_accepts_api_modes() -> None:
    parser = _build_parser()
    args = parser.parse_args(["--source-mode", "api_shadow"])
    assert args.source_mode == "api_shadow"


def test_extract_rows_handles_common_response_shapes() -> None:
    assert _extract_rows([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]
    assert _extract_rows({"items": [{"a": 1}], "x": 1}) == [{"a": 1}]
    assert _extract_rows({"data": {"rows": [{"x": 9}]}}) == [{"x": 9}]
    assert _extract_rows({"data": "bad"}) == []


def test_api_client_reads_storage_state_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "store-state.json"
    artifact.write_text(json.dumps({"cookies": [{"name": "session"}], "origins": []}), encoding="utf-8")
    client = TdApiClient(store_code="a123", context=None, storage_state_path=artifact)  # type: ignore[arg-type]
    state = client.read_session_artifact()
    assert state["cookies"][0]["name"] == "session"


def test_normalize_garment_rows_surfaces_ids_and_line_keys() -> None:
    rows = _normalize_garment_rows([{"orderNo": "ORD-1", "lineItemId": "L1", "garmentId": "G1", "lineItemKey": "LK1"}])
    assert rows[0]["order_number"] == "ORD-1"
    assert rows[0]["api_line_item_id"] == "L1"
    assert rows[0]["api_garment_id"] == "G1"
    assert rows[0]["line_item_key"] == "LK1"
