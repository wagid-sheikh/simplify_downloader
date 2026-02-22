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
    _filter_summary_rows,
)
from app.crm_downloader.td_orders_sync.td_api_compare import (
    COMPARE_KEY_FIELDS_BY_DATASET,
    compare_canonical_rows,
    project_api_rows_for_compare,
)


ORDERS_IGNORABLE_API_ONLY_FIELDS = {"bookingSlipUrl", "storeName", "deliveryDate"}
SALES_IGNORABLE_API_ONLY_FIELDS = {"printer", "storeName", "storeId"}


@pytest.fixture
def representative_orders_api_row() -> dict[str, str]:
    return {
        "orderNo": "O-1001",
        "orderDate": "2026-01-02 10:00:00",
        "amount": "120.50",
        "status": "Delivered",
        "bookingSlipUrl": "https://files.example/booking-slip/O-1001",
        "storeName": "Andheri",
        "deliveryDate": "2026-01-03",
    }


@pytest.fixture
def representative_sales_api_row() -> dict[str, str]:
    return {
        "orderNo": "S-2001",
        "paymentDate": "2026-01-02 11:00:00",
        "paymentMode": "UPI",
        "amount": "80.00",
        "status": "Collected",
        "printer": "FrontDesk-01",
        "storeName": "Andheri",
        "storeId": "A817",
    }


def test_source_mode_parser_accepts_api_modes() -> None:
    parser = _build_parser()
    args = parser.parse_args(["--source-mode", "api_shadow"])
    assert args.source_mode == "api_shadow"


def test_extract_rows_handles_common_response_shapes() -> None:
    assert _extract_rows([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]
    assert _extract_rows({"items": [{"a": 1}], "x": 1}) == [{"a": 1}]
    assert _extract_rows({"data": {"rows": [{"x": 9}]}}) == [{"x": 9}]
    assert _extract_rows({"data": "bad"}) == []




def test_filter_summary_rows_removes_footer_like_rows() -> None:
    rows = [
        {"orderNumber": "A-1001", "customerName": "Alice", "totalAmount": "120.00"},
        {"orderNumber": None, "customerName": "Total Order", "totalAmount": "120.00"},
        {"orderNo": "S-2002", "paymentDate": "2026-01-02", "netAmount": "80.00"},
        {"orderNo": "", "description": "Grand Total", "netAmount": "200.00", "tax": "20.00"},
        {"orderNumber": 500, "orderDate": "Total Order", "netAmount": "200.00"},
    ]

    filtered_rows, summary_rows_filtered = _filter_summary_rows(rows)

    assert summary_rows_filtered == 3
    assert filtered_rows == [
        {"orderNumber": "A-1001", "customerName": "Alice", "totalAmount": "120.00"},
        {"orderNo": "S-2002", "paymentDate": "2026-01-02", "netAmount": "80.00"},
    ]


def test_api_client_reads_storage_state_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "store-state.json"
    artifact.write_text(json.dumps({"cookies": [{"name": "session"}], "origins": []}), encoding="utf-8")
    client = TdApiClient(store_code="a123", context=None, storage_state_path=artifact)  # type: ignore[arg-type]
    state = client.read_session_artifact()
    assert state["cookies"][0]["name"] == "session"


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







def test_orders_projection_contract_required_fields_and_allow_list(
    representative_orders_api_row: dict[str, str],
) -> None:
    projected = project_api_rows_for_compare(
        dataset="orders",
        api_rows=[representative_orders_api_row],
        store_code="a817",
    )[0]

    required_fields = {"store_code", "order_number", "order_date", "amount", "status"}
    assert required_fields.issubset(projected.keys())
    assert projected["store_code"] == "A817"
    assert projected["order_number"] == representative_orders_api_row["orderNo"]
    assert projected["order_date"] == representative_orders_api_row["orderDate"]
    assert projected["amount"] == representative_orders_api_row["amount"]
    assert projected["status"] == representative_orders_api_row["status"]

    ui_equivalent_api_keys = {
        "orderNo",
        "orderDate",
        "amount",
        "status",
    }
    allow_list = ORDERS_IGNORABLE_API_ONLY_FIELDS
    unmapped_required = [
        key
        for key in representative_orders_api_row.keys()
        if key not in ui_equivalent_api_keys and key not in allow_list
    ]
    assert unmapped_required == [], f"Orders API keys require mapping or allow-listing: {unmapped_required}"


def test_sales_projection_contract_required_fields_and_allow_list(
    representative_sales_api_row: dict[str, str],
) -> None:
    projected = project_api_rows_for_compare(
        dataset="sales",
        api_rows=[representative_sales_api_row],
        store_code="a817",
    )[0]

    required_fields = {"store_code", "order_number", "payment_date", "payment_mode", "amount", "status"}
    assert required_fields.issubset(projected.keys())
    assert projected["store_code"] == "A817"
    assert projected["order_number"] == representative_sales_api_row["orderNo"]
    assert projected["payment_date"] == representative_sales_api_row["paymentDate"]
    assert projected["payment_mode"] == representative_sales_api_row["paymentMode"]
    assert projected["amount"] == representative_sales_api_row["amount"]
    assert projected["status"] == representative_sales_api_row["status"]

    ui_equivalent_api_keys = {
        "orderNo",
        "paymentDate",
        "paymentMode",
        "amount",
        "status",
    }
    allow_list = SALES_IGNORABLE_API_ONLY_FIELDS
    unmapped_required = [
        key
        for key in representative_sales_api_row.keys()
        if key not in ui_equivalent_api_keys and key not in allow_list
    ]
    assert unmapped_required == [], f"Sales API keys require mapping or allow-listing: {unmapped_required}"


def test_projection_round_trip_preserves_required_contract_fields(
    representative_orders_api_row: dict[str, str],
    representative_sales_api_row: dict[str, str],
) -> None:
    orders_projected = project_api_rows_for_compare(
        dataset="orders",
        api_rows=[representative_orders_api_row],
        store_code="a817",
    )[0]
    sales_projected = project_api_rows_for_compare(
        dataset="sales",
        api_rows=[representative_sales_api_row],
        store_code="a817",
    )[0]

    orders_api_path_output = {**representative_orders_api_row, **orders_projected}
    sales_api_path_output = {**representative_sales_api_row, **sales_projected}

    assert orders_api_path_output["order_number"] == representative_orders_api_row["orderNo"]
    assert orders_api_path_output["order_date"] == representative_orders_api_row["orderDate"]
    assert orders_api_path_output["amount"] == representative_orders_api_row["amount"]
    assert orders_api_path_output["status"] == representative_orders_api_row["status"]

    assert sales_api_path_output["order_number"] == representative_sales_api_row["orderNo"]
    assert sales_api_path_output["payment_date"] == representative_sales_api_row["paymentDate"]
    assert sales_api_path_output["payment_mode"] == representative_sales_api_row["paymentMode"]
    assert sales_api_path_output["amount"] == representative_sales_api_row["amount"]
    assert sales_api_path_output["status"] == representative_sales_api_row["status"]

def test_project_api_rows_for_compare_isolated_projection() -> None:
    projected = project_api_rows_for_compare(
        dataset="orders",
        api_rows=[{"orderNo": "1001", "orderDate": "2026-01-02 10:00:00", "amount": "12", "status": "Delivered", "sourceOnly": "keep"}],
        store_code="a817",
    )

    assert projected == [
        {
            "store_code": "A817",
            "order_number": "1001",
            "order_date": "2026-01-02 10:00:00",
            "amount": "12",
            "status": "Delivered",
        }
    ]

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
        order_rows=[{"order_number": "1001", "source_only": "x", "amount": "12.00"}],
        sale_rows=[{"order_number": "1001", "sales_only": "y", "amount": "12.00"}],
        garments_rows=[{"order_number": "1001", "garment_only": "z", "line_item_key": "L1"}],
    )

    assert "orders_excel" in result.artifact_paths
    assert "sales_excel" in result.artifact_paths
    assert "garments_excel" in result.artifact_paths
    assert Path(result.artifact_paths["orders_excel"]).exists()
    orders_rows_path = Path(result.artifact_paths["orders_rows"])
    assert orders_rows_path.exists()
    assert '"source_only": "x"' in orders_rows_path.read_text(encoding="utf-8")


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
                payload={"data": [{"orderNo": "S-1", "paymentDate": "2026-01-02 10:00:00", "total": "10", "salesOnlyField": "survives"}], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/garments/details",
                payload={"data": [{"orderNo": "G-1", "lineItemId": "L1", "garmentOnlyField": "survives"}], "totalPages": 1},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")

    result = await client.fetch_reports(from_date=date(2026, 1, 1), to_date=date(2026, 1, 2))

    assert result.raw_orders_payload["error"] == "http_403"
    assert result.raw_sales_payload["error"] is None
    assert result.raw_garments_payload["error"] is None
    assert result.orders_rows == []
    assert len(result.sales_rows) == 1
    assert len(result.garments_rows) == 1
    assert result.sales_rows[0]["salesOnlyField"] == "survives"
    assert result.garments_rows[0]["garmentOnlyField"] == "survives"


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


@pytest.mark.asyncio
async def test_fetch_reports_preserves_full_fidelity_rows_for_artifacts(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={"data": [{"orderNo": "O-1", "ordersOnlyField": "keep"}], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={"data": [{"orderNo": "S-1", "salesOnlyField": "keep"}], "totalPages": 1},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/garments/details",
                payload={"data": [{"orderNo": "G-1", "garmentsOnlyField": "keep"}], "totalPages": 1},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")

    result = await client.fetch_reports(from_date=date(2026, 1, 1), to_date=date(2026, 1, 2))

    assert result.orders_rows[0]["ordersOnlyField"] == "keep"
    assert result.sales_rows[0]["salesOnlyField"] == "keep"
    assert result.garments_rows[0]["garmentsOnlyField"] == "keep"
    assert result.raw_orders_payload["data"][0]["ordersOnlyField"] == "keep"
    assert result.raw_sales_payload["data"][0]["salesOnlyField"] == "keep"
    assert result.raw_garments_payload["data"][0]["garmentsOnlyField"] == "keep"


@pytest.mark.asyncio
async def test_fetch_reports_filters_summary_rows_from_orders_and_sales(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={
                    "data": [
                        {"orderNumber": "1001", "customer": "Alice", "amount": "10.00"},
                        {"orderNumber": "", "description": "Total", "amount": "10.00"},
                    ],
                    "totalPages": 1,
                },
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={
                    "data": [
                        {"orderNo": "S-1", "paymentDate": "2026-01-02", "netAmount": "7.00"},
                        {"orderNo": None, "label": "Grand Total", "netAmount": "7.00", "tax": "1.00"},
                    ],
                    "totalPages": 1,
                },
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

    assert [row["orderNumber"] for row in result.orders_rows] == ["1001"]
    assert [row["orderNo"] for row in result.sales_rows] == ["S-1"]


@pytest.mark.asyncio
async def test_fetch_reports_exposes_summary_filter_counts(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={
                    "data": [
                        {"orderNumber": "1001", "customer": "Alice", "amount": "10.00"},
                        {"orderNumber": "", "description": "Total", "amount": "10.00"},
                    ],
                    "totalPages": 1,
                },
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={
                    "data": [
                        {"orderNo": "S-1", "paymentDate": "2026-01-02", "netAmount": "7.00"},
                        {"orderNo": None, "label": "Grand Total", "netAmount": "7.00", "tax": "1.00"},
                    ],
                    "totalPages": 1,
                },
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

    assert result.orders_summary_rows_filtered == 1
    assert result.sales_summary_rows_filtered == 1


@pytest.mark.asyncio
async def test_fetch_reports_honors_td_api_max_pages_cap(tmp_path: Path) -> None:
    request = _StubRequest(
        responses=[
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/reports/order-report",
                payload={"data": [{"orderNumber": "1001"}], "totalPages": 3, "total": 3},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/sales-and-deliveries/sales",
                payload={"data": [{"orderNo": "S-1"}], "totalPages": 3, "total": 3},
            ),
            _StubResponse(
                status=200,
                url="https://reporting-api.quickdrycleaning.com/garments/details",
                payload={"data": [{"orderNo": "G-1"}], "totalPages": 3, "total": 3},
            ),
        ]
    )
    context = _StubContext(request=request)
    client = _TokenRefreshingClient(store_code="a123", context=context, storage_state_path=tmp_path / "s.json")
    client.config = type(client.config)(max_pages=1)

    result = await client.fetch_reports(from_date=date(2026, 1, 1), to_date=date(2026, 1, 2))

    assert len(request.calls) == 3
    assert result.raw_orders_payload["pagination"]["pages_fetched"] == 1
    assert result.raw_sales_payload["pagination"]["pages_fetched"] == 1
    assert result.raw_garments_payload["pagination"]["pages_fetched"] == 1
