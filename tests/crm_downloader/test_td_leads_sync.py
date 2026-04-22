from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.crm_downloader.td_leads_sync.ingest import build_lead_uid
from app.crm_downloader.td_leads_sync.main import _ensure_scheduler_page, _field_from_headers


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
