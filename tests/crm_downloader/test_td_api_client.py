from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.crm_downloader.td_orders_sync.main import (
    _build_parser,
    _persist_compare_excel_artifact,
    _resolve_td_api_artifact_dir,
)
from app.crm_downloader.td_orders_sync.td_api_artifacts import persist_td_api_artifacts
from app.crm_downloader.td_orders_sync.td_api_client import (
    TdApiClient,
    _extract_rows,
    _normalize_garment_rows,
    _normalize_order_rows,
    _normalize_sales_rows,
)
from app.crm_downloader.td_orders_sync.td_api_compare import COMPARE_KEY_FIELDS_BY_DATASET, compare_canonical_rows


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
    rows = _normalize_garment_rows([{"orderNo": "ORD-1", "lineItemId": "L1", "garmentId": "G1", "lineItemKey": "LK1"}], store_code="a123")
    assert rows[0]["order_number"] == "ORD-1"
    assert rows[0]["api_line_item_id"] == "L1"
    assert rows[0]["api_garment_id"] == "G1"
    assert rows[0]["line_item_key"] == "LK1"


def test_api_normalizers_align_datetime_and_numeric_precision() -> None:
    orders = _normalize_order_rows(
        [{"orderNo": "ORD-1", "orderDate": "2026-01-02 10:00:00", "amount": "12"}], store_code="a817"
    )
    sales = _normalize_sales_rows(
        [{"orderNo": "ORD-1", "paymentDate": "2026-01-02 10:00:00", "total": "12"}], store_code="a817"
    )

    assert orders[0]["order_date"].startswith("2026-01-02T10:00:00")
    assert orders[0]["amount"] == "12.00"
    assert sales[0]["payment_date"].startswith("2026-01-02T10:00:00")
    assert sales[0]["amount"] == "12.00"


def test_compare_uses_identical_canonical_key_and_emits_mismatch_artifacts() -> None:
    ui_rows = [
        {
            "store_code": "A817",
            "order_number": "1001",
            "order_date": "2026-01-02 10:00:00",
            "amount": "12",
            "status": "Delivered",
        },
        {
            "store_code": "A817",
            "order_number": "1002",
            "order_date": "2026-01-02 11:00:00",
            "amount": "8",
            "status": "Delivered",
        },
    ]
    api_rows = [
        {
            "store_code": "A817",
            "order_number": "1001",
            "order_date": "2026-01-02T10:00:00+05:30",
            "amount": "12.00",
            "status": "delivered",
        },
        {
            "store_code": "A817",
            "order_number": "1003",
            "order_date": "2026-01-02T11:30:00+05:30",
            "amount": "8.00",
            "status": "delivered",
        },
    ]

    metrics = compare_canonical_rows(
        ui_rows=ui_rows,
        api_rows=api_rows,
        key_fields=COMPARE_KEY_FIELDS_BY_DATASET["orders"],
    ).as_dict()

    assert metrics["matched_rows"] == 1
    assert metrics["missing_in_api"] == 1
    assert metrics["missing_in_ui"] == 1
    assert metrics["amount_mismatches"] == 0
    assert metrics["status_mismatches"] == 0
    assert metrics["mismatch_artifacts"]["missing_in_api"][0]["key_components"]["order_number"] == "1002"
    assert metrics["mismatch_artifacts"]["missing_in_ui"][0]["key_components"]["order_number"] == "1003"




def test_resolve_td_api_artifact_dir_defaults_and_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TD_API_ARTIFACT_DIR", raising=False)
    from app.crm_downloader.config import default_download_dir

    assert _resolve_td_api_artifact_dir() == default_download_dir().resolve()

    custom_dir = tmp_path / "custom-artifacts"
    monkeypatch.setenv("TD_API_ARTIFACT_DIR", str(custom_dir))
    assert _resolve_td_api_artifact_dir() == custom_dir.resolve()


def test_compare_excel_summary_excludes_nested_mismatch_artifacts(tmp_path: Path) -> None:
    artifact_path = tmp_path / "compare.xlsx"
    compare_metrics = {
        "total_rows": 2,
        "matched_rows": 1,
        "missing_in_api": 1,
        "missing_in_ui": 0,
        "amount_mismatches": 0,
        "status_mismatches": 0,
        "sample_mismatch_keys": "A817|1002",
        "mismatch_artifacts": {"missing_in_api": [{"order_number": "1002"}]},
    }

    _persist_compare_excel_artifact(
        artifact_path=artifact_path,
        compare_metrics=compare_metrics,
        api_request_metadata=[],
    )

    import openpyxl

    workbook = openpyxl.load_workbook(artifact_path)
    summary = workbook["summary"]
    headers = [cell.value for cell in next(summary.iter_rows(min_row=1, max_row=1))]
    assert "mismatch_artifacts" not in headers




def test_persist_td_api_artifacts_writes_excel_outputs(tmp_path: Path) -> None:
    result = persist_td_api_artifacts(
        download_dir=tmp_path,
        store_code="a817",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
        raw_orders={"data": []},
        raw_sales={"data": []},
        raw_garments={"data": []},
        canonical_orders=[{"order_number": "1001", "amount": "12.00"}],
        canonical_sales=[{"order_number": "1001", "amount": "12.00"}],
        canonical_garments=[{"order_number": "1001", "line_item_key": "L1"}],
    )

    assert "orders_excel" in result.artifact_paths
    assert "sales_excel" in result.artifact_paths
    assert "garments_excel" in result.artifact_paths
    assert Path(result.artifact_paths["orders_excel"]).exists()


class _StubResponse:
    def __init__(self, *, status: int, url: str, payload: dict[str, object]) -> None:
        self.status = status
        self.url = url
        self._payload = payload

    async def json(self) -> dict[str, object]:
        return self._payload


class _StubRequest:
    def __init__(self, responses: list[_StubResponse]) -> None:
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    async def get(self, url: str, params: object, headers: dict[str, str], timeout: int) -> _StubResponse:
        self.calls.append({"url": url, "params": params, "headers": dict(headers), "timeout": timeout})
        return self._responses.pop(0)


class _StubContext:
    def __init__(self, request: _StubRequest) -> None:
        self.request = request
        self.pages: list[object] = []


class _TokenRefreshingClient(TdApiClient):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.discovery_calls: list[bool] = []

    async def _discover_reporting_token(self, *, force_refresh: bool = False):  # type: ignore[override]
        self.discovery_calls.append(force_refresh)
        if force_refresh:
            return type(self)._token_result("fresh-token")
        return type(self)._token_result("stale-token")

    @staticmethod
    def _token_result(token: str):
        from app.crm_downloader.td_orders_sync.td_api_client import _TokenDiscoveryResult

        return _TokenDiscoveryResult(token=token, source="test", expiry=None)


@pytest.mark.asyncio
async def test_get_json_retries_once_after_401_with_refreshed_token(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(status=401, url="https://reporting-api.quickdrycleaning.com/reports/order-report", payload={}),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={"data": [{"id": 1}]},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")

    metadata: list[dict[str, object]] = []
    result = await client._get_json(endpoint="/reports/order-report", params={"page": 1}, metadata=metadata)

    assert result.ok is True
    assert result.payload == {"data": [{"id": 1}]}
    assert len(request.calls) == 2
    assert request.calls[0]["headers"].get("Authorization") == "Bearer stale-token"
    assert request.calls[1]["headers"].get("Authorization") == "Bearer fresh-token"
    assert client.discovery_calls == [False, True]
    assert metadata[0]["status"] == 401
    assert metadata[0]["retry_count"] == 0
    assert metadata[0]["token_refresh_attempted"] is True
    assert metadata[1]["status"] == 200
    assert metadata[1]["token_refresh_attempted"] is True


@pytest.mark.asyncio
async def test_fetch_reports_captures_non_retriable_http_errors_per_endpoint(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(status=403, url="https://reporting-api.quickdrycleaning.com/reports/order-report", payload={}),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={"data": [{"orderNo": "S-1", "paymentDate": "2026-01-02 10:00:00", "total": "10"}], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/garments/details",
                payload={"data": [{"orderNo": "G-1", "lineItemId": "L1"}], "totalPages": 1},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")

    result = await client.fetch_reports(from_date=date(2026, 1, 1), to_date=date(2026, 1, 2))

    assert result.raw_orders_payload["error"] == "http_403"
    assert result.raw_sales_payload["error"] is None
    assert result.raw_garments_payload["error"] is None
    assert result.normalized_orders == []
    assert len(result.normalized_sales) == 1
    assert len(result.normalized_garments) == 1


@pytest.mark.asyncio
async def test_fetch_reports_sets_expand_data_true_for_orders_and_sales(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={"data": [], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={"data": [], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/garments/details",
                payload={"data": [], "totalPages": 1},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")

    await client.fetch_reports(from_date=date(2026, 1, 1), to_date=date(2026, 1, 2))

    orders_call = next(call for call in request.calls if call["url"].endswith("/reports/order-report"))
    sales_call = next(call for call in request.calls if call["url"].endswith("/sales-and-deliveries/sales"))
    garments_call = next(call for call in request.calls if call["url"].endswith("/garments/details"))

    assert orders_call["params"]["expandData"] == "true"
    assert sales_call["params"]["expandData"] == "true"
    assert "expandData" in orders_call["params"]
    assert "expandData" in sales_call["params"]
    assert "expandData" not in garments_call["params"]
