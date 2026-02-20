from __future__ import annotations

import json
from pathlib import Path

import pytest

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
    payload = await client._get_json(endpoint="/reports/order-report", params={"page": 1}, metadata=metadata)

    assert payload == {"data": [{"id": 1}]}
    assert len(request.calls) == 2
    assert request.calls[0]["headers"].get("Authorization") == "Bearer stale-token"
    assert request.calls[1]["headers"].get("Authorization") == "Bearer fresh-token"
    assert client.discovery_calls == [False, True]
    assert metadata[0]["status"] == 401
    assert metadata[0]["retry_count"] == 0
    assert metadata[0]["token_refresh_attempted"] is True
    assert metadata[1]["status"] == 200
    assert metadata[1]["token_refresh_attempted"] is True
