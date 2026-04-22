from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.crm_downloader.td_leads_sync.ingest import build_lead_uid
from app.crm_downloader.td_leads_sync.main import (
    _available_pager_args,
    _ensure_scheduler_page,
    _field_from_headers,
    _find_tz_aware_columns,
    _postback_page_arg,
    _sanitize_rows_for_xlsx_export,
    _scrape_grid_rows,
    _write_store_artifact,
)


def test_build_lead_uid_is_stable_for_same_business_identity() -> None:
    base = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    uid_a = build_lead_uid(base)
    uid_b = build_lead_uid(dict(base))
    assert uid_a == uid_b


def test_build_lead_uid_changes_when_status_changes() -> None:
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    pending_uid = build_lead_uid(row)
    cancelled_uid = build_lead_uid({**row, "status_bucket": "cancelled"})
    assert pending_uid != cancelled_uid


@pytest.mark.parametrize(
    ("field_name", "expected"),
    [
        ("pickup_no", "A668-3025"),
        ("customer_name", "moni"),
        ("mobile", "9599242207"),
        ("pickup_date", "22 Apr 2026"),
        ("pickup_time", "11:00 AM - 1:00 PM"),
        ("special_instruction", "Leave at door"),
        ("status_text", "CANCELLED"),
        ("reason", "enquiry"),
        ("source", "Facebook"),
        ("user", "Super Admin"),
    ],
)
def test_field_from_headers_uses_header_name_mapping(field_name: str, expected: str) -> None:
    headers = [
        "S No.",
        "Pickup No.",
        "Customer Name",
        "Address",
        "Mobile",
        "Pickup Date",
        "Pickup Time",
        "Special Instruction",
        "Status",
        "Reason",
        "Source",
        "User",
    ]
    values = [
        "1",
        "A668-3025",
        "moni",
        "Address line",
        "9599242207",
        "22 Apr 2026",
        "11:00 AM - 1:00 PM",
        "Leave at door",
        "CANCELLED",
        "enquiry",
        "Facebook",
        "Super Admin",
    ]

    resolved = _field_from_headers(headers=headers, values=values, field_name=field_name)
    assert resolved == expected


def test_field_from_headers_returns_none_when_alias_not_present() -> None:
    assert _field_from_headers(headers=["Foo"], values=["Bar"], field_name="pickup_no") is None


def test_scraped_at_value_can_pass_through() -> None:
    now_utc = datetime.now(timezone.utc)
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "1",
        "pickup_no": "A668-1",
        "mobile": "9999999999",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
        "scraped_at": now_utc,
    }
    assert build_lead_uid(row)


def test_sanitize_rows_for_xlsx_export_converts_tz_aware_datetime_and_iso_strings() -> None:
    aware_value = datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc)
    rows = [
        {
            "pickup_date": "2026-04-22T11:00:00+05:30",
            "scraped_at": aware_value,
            "mobile": "9999999999",
        }
    ]

    sanitized = _sanitize_rows_for_xlsx_export(rows=rows)

    assert sanitized[0]["pickup_date"] == datetime(2026, 4, 22, 11, 0)
    assert sanitized[0]["scraped_at"] == datetime(2026, 4, 22, 6, 30)
    assert sanitized[0]["scraped_at"].tzinfo is None
    assert rows[0]["scraped_at"] is aware_value
    assert rows[0]["pickup_date"] == "2026-04-22T11:00:00+05:30"


def test_find_tz_aware_columns_flags_remaining_tz_values() -> None:
    rows = [
        {
            "pickup_date": "22 Apr 2026",
            "scraped_at": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
            "mobile": "9999999999",
        }
    ]

    tz_columns = _find_tz_aware_columns(rows=rows, columns=["pickup_date", "scraped_at", "mobile"])

    assert tz_columns == {"scraped_at"}


def test_write_store_artifact_fails_when_tz_aware_values_remain(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )
    rows = [
        {
            "store_code": "A668",
            "status_bucket": "pending",
            "pickup_id": "1",
            "pickup_no": "A668-1",
            "customer_name": "Foo",
            "address": "Bar",
            "mobile": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
            "pickup_date": "22 Apr 2026",
            "pickup_time": "11:00 AM - 1:00 PM",
            "special_instruction": "",
            "status_text": "PENDING",
            "reason": "",
            "source": "",
            "user": "",
            "scraped_at": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
        }
    ]

    with pytest.raises(ValueError, match="timezone-aware datetime values in columns: mobile"):
        _write_store_artifact(
            store_code="A668",
            rows=rows,
            output_dir=tmp_path,
            logger=SimpleNamespace(),
        )

    assert events
    assert events[-1]["tz_aware_columns"] == ["mobile"]


def test_write_store_artifact_writes_temp_then_promotes(tmp_path) -> None:
    rows = [
        {
            "store_code": "A668",
            "status_bucket": "pending",
            "pickup_id": "1",
            "pickup_no": "A668-1",
            "customer_name": "Foo",
            "address": "Bar",
            "mobile": "9999999999",
            "pickup_date": "22 Apr 2026",
            "pickup_time": "11:00 AM - 1:00 PM",
            "special_instruction": "",
            "status_text": "PENDING",
            "reason": "",
            "source": "",
            "user": "",
            "scraped_at": datetime(2026, 4, 22, 6, 30),
        }
    ]

    output_path = _write_store_artifact(
        store_code="A668",
        rows=rows,
        output_dir=tmp_path,
        logger=SimpleNamespace(),
    )

    assert output_path == tmp_path / "A668-crm_leads.xlsx"
    assert output_path.exists()
    assert not (tmp_path / "A668-crm_leads.xlsx.tmp").exists()


def test_write_store_artifact_removes_temp_and_logs_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    class _FakeSheet:
        def __init__(self) -> None:
            self.title = ""

        def append(self, _row) -> None:
            return None

    class _FailingWorkbook:
        def __init__(self) -> None:
            self.active = _FakeSheet()

        def save(self, _path) -> None:
            raise OSError("disk full")

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.openpyxl.Workbook",
        lambda: _FailingWorkbook(),
    )

    with pytest.raises(OSError, match="disk full"):
        _write_store_artifact(
            store_code="A668",
            rows=[],
            output_dir=tmp_path,
            logger=SimpleNamespace(),
        )

    tmp_artifact = tmp_path / "A668-crm_leads.xlsx.tmp"
    assert not tmp_artifact.exists()
    assert events
    assert events[-1]["message"] == "artifact_write_failed"
    assert events[-1]["store_code"] == "A668"
    assert events[-1]["artifact_path"] == str(tmp_artifact)


class _FakeLocator:
    def __init__(self, count: int) -> None:
        self._count = count

    async def count(self) -> int:
        return self._count


class _FakeNavigationContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakePage:
    def __init__(self, *, selectors_present: set[str], url: str = "https://subs.quickdrycleaning.com/a668/App/home") -> None:
        self.selectors_present = selectors_present
        self.url = url
        self.waited_selectors: list[str] = []
        self.clicked: list[str] = []
        self.goto_urls: list[str] = []
        self.waited_url_patterns: list[object] = []
        self.expect_navigation_calls = 0
        self.title_text = "Pickup Scheduler"
        self.fail_ready = False

    async def wait_for_selector(self, selector: str, timeout: int | None = None) -> None:
        self.waited_selectors.append(selector)
        if self.fail_ready and selector in {"#drpStatus", "#grdEntry", "#grdCompleted", "#grdCanceled"}:
            raise TimeoutError("status selector timeout")

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(1 if selector in self.selectors_present else 0)

    def expect_navigation(self, **kwargs) -> _FakeNavigationContext:
        self.expect_navigation_calls += 1
        return _FakeNavigationContext()

    async def click(self, selector: str) -> None:
        self.clicked.append(selector)
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def goto(self, url: str, **kwargs) -> None:
        self.goto_urls.append(url)
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def wait_for_url(self, pattern, **kwargs) -> None:
        self.waited_url_patterns.append(pattern)

    async def title(self) -> str:
        return self.title_text


@pytest.mark.asyncio
async def test_ensure_scheduler_page_prefers_pickup_alert_click(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp"})
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.clicked == ["#achrPickUp"]
    assert not page.goto_urls
    assert page.expect_navigation_calls == 1
    assert any(event.get("navigation_branch") == "home_alert_click" for event in events)


@pytest.mark.asyncio
async def test_ensure_scheduler_page_uses_fallback_click_when_alert_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    fallback = "a[href*='frmHomePickUpScheduler.aspx']"
    page = _FakePage(selectors_present={fallback})
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.clicked == [fallback]
    assert not page.goto_urls
    assert any(str(event.get("navigation_branch", "")).startswith("fallback_click:") for event in events)


@pytest.mark.asyncio
async def test_ensure_scheduler_page_timeout_logs_selector_and_final_url(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp"})
    page.fail_ready = True
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is False
    error_events = [event for event in events if event.get("status") == "error"]
    assert error_events
    error_event = error_events[-1]
    assert error_event.get("awaited_selectors")
    assert str(error_event.get("final_url", "")).endswith("frmHomePickUpScheduler.aspx")


class _FakeEvaluatePage:
    def __init__(self, evaluate_result):
        self.evaluate_result = evaluate_result
        self.evaluate_calls: list[tuple[str, dict[str, str]]] = []

    async def evaluate(self, script: str, payload: dict[str, str]):
        self.evaluate_calls.append((script, payload))
        return self.evaluate_result


@pytest.mark.asyncio
async def test_available_pager_args_uses_raw_regex_pattern_in_evaluate_script() -> None:
    page = _FakeEvaluatePage(["Page$1", "Page$2"])

    values = await _available_pager_args(page, grid_selector="#grdEntry")

    assert values == ["Page$1", "Page$2"]
    script, payload = page.evaluate_calls[0]
    assert payload == {"gridSelector": "#grdEntry"}
    assert r"href.match(/Page\$\d+/i)" in script


@pytest.mark.asyncio
async def test_postback_page_arg_uses_event_argument_payload() -> None:
    page = _FakeEvaluatePage(None)

    await _postback_page_arg(page, arg="Page$3")

    script, payload = page.evaluate_calls[0]
    assert payload == {"eventArgument": "Page$3"}
    assert "__EVENTARGUMENT" in script
    assert "__doPostBack('', eventArgument)" in script


@pytest.mark.asyncio
async def test_scrape_grid_rows_script_keeps_whitespace_regex_literal() -> None:
    page = _FakeEvaluatePage({"headers": [], "rows": []})

    headers, rows = await _scrape_grid_rows(page, grid_selector="#grdEntry")

    assert headers == []
    assert rows == []
    script, _payload = page.evaluate_calls[0]
    assert r"replace(/\s+/g, ' ')" in script
