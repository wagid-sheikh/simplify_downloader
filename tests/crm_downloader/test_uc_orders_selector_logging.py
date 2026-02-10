import io
import asyncio
import json

from types import SimpleNamespace

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


@pytest.mark.asyncio
async def test_selector_cue_logging_skipped_when_dom_logging_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    monkeypatch.setattr(
        uc_main, "config", SimpleNamespace(pipeline_skip_dom_logging=True)
    )

    await uc_main._log_selector_cues(
        logger=logger, store_code="A100", container=object(), page=object()
    )

    log = json.loads(output.getvalue())
    assert log["phase"] == "selectors"
    assert log["message"].startswith("Skipped GST report selector cue capture")
    assert log["store_code"] == "A100"
    assert "controls" not in log
    assert "spinners" not in log


def test_normalize_order_info_key_handles_punctuation_and_spacing() -> None:
    assert uc_main._normalize_order_info_key(" Order Date : ") == "order date"
    assert uc_main._normalize_order_info_key("Order No. - UC123") == "order no - uc123"


def test_normalize_order_info_key_handles_empty_values() -> None:
    assert uc_main._normalize_order_info_key(None) == ""
    assert uc_main._normalize_order_info_key("   ") == ""


def test_parse_archive_footer_window_extracts_start_end_total() -> None:
    assert uc_main._parse_archive_footer_window(
        "Showing results 1 to 30 of 785 total"
    ) == (1, 30, 785)
    assert uc_main._parse_archive_footer_window(
        " showing results 31 to 60 of 785 total "
    ) == (31, 60, 785)


def test_parse_archive_footer_window_returns_none_for_non_matching_text() -> None:
    assert uc_main._parse_archive_footer_window("Showing page 1") is None


class _FakeRowLocator:
    def __init__(self, page):
        self._page = page

    async def count(self) -> int:
        return len(self._page.pages[self._page.index]["order_codes"])

    def nth(self, idx: int):
        return SimpleNamespace(
            order_code=self._page.pages[self._page.index]["order_codes"][idx]
        )


class _FakeNextButton:
    def __init__(self, page):
        self._page = page

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return 1

    async def is_disabled(self) -> bool:
        return bool(self._page.pages[self._page.index].get("next_disabled", False))

    async def get_attribute(self, _: str):
        return None

    async def click(self) -> None:
        remaining_stalls = int(
            self._page.pages[self._page.index].get("stall_next_clicks", 0)
        )
        if remaining_stalls > 0:
            self._page.pages[self._page.index]["stall_next_clicks"] = (
                remaining_stalls - 1
            )
            return
        if self._page.index < len(self._page.pages) - 1:
            self._page.index += 1

    async def scroll_into_view_if_needed(self, timeout: int | None = None) -> None:
        self._page.scroll_calls += 1


class _FakePage:
    def __init__(self, pages):
        self.pages = pages
        self.index = 0
        self.scroll_calls = 0
        self.timeout_calls = 0

    def locator(self, selector: str):
        if selector == uc_main.ARCHIVE_TABLE_ROW_SELECTOR:
            return _FakeRowLocator(self)
        if selector == uc_main.ARCHIVE_NEXT_BUTTON_SELECTOR:
            return _FakeNextButton(self)
        return _FakeNextButton(self)

    async def wait_for_selector(self, *args, **kwargs):
        return None

    async def wait_for_function(self, *args, **kwargs):
        return None

    async def wait_for_timeout(self, _ms: int):
        self.timeout_calls += 1
        return None


@pytest.mark.asyncio
async def test_collect_archive_orders_does_not_stop_on_page1_when_footer_has_more(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 785),
            "next_disabled": False,
        },
        {
            "order_codes": [f"O{i:03d}" for i in range(31, 61)],
            "footer": (31, 60, 785),
            "next_disabled": True,
        },
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]),
    )
    monkeypatch.setattr(
        uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None)
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_first_order_code",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_page_number",
        lambda _p: asyncio.sleep(0, result=page.index + 1),
    )
    monkeypatch.setattr(
        uc_main,
        "_is_button_disabled",
        lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert extract.page_count == 2
    assert len(extract.base_rows) == 60
    assert extract.skipped_order_counters["footer_baseline_unavailable"] == 1


@pytest.mark.asyncio
async def test_collect_archive_orders_forces_retry_when_duplicates_but_footer_has_more(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 48),
            "next_disabled": False,
        },
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 48),
            "next_disabled": False,
        },
        {
            "order_codes": [f"O{i:03d}" for i in range(31, 49)],
            "footer": (31, 48, 48),
            "next_disabled": True,
        },
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]),
    )
    monkeypatch.setattr(
        uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None)
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_first_order_code",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_page_number",
        lambda _p: asyncio.sleep(0, result=page.index + 1),
    )
    monkeypatch.setattr(
        uc_main,
        "_is_button_disabled",
        lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert len(extract.base_rows) == 48
    assert extract.page_count == 3
    assert page.scroll_calls >= 1
    assert page.timeout_calls >= 1


@pytest.mark.asyncio
async def test_collect_archive_orders_continues_when_single_row_read_fails_on_partial_last_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 48),
            "next_disabled": False,
        },
        {
            "order_codes": [f"O{i:03d}" for i in range(31, 49)],
            "footer": (31, 48, 48),
            "next_disabled": True,
        },
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    async def _extract_archive_base_row(**kwargs):
        if kwargs["order_code"] == "O048":
            raise RuntimeError("detached row")
        return {"order_code": kwargs["order_code"], "payment_text": "paid"}

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]),
    )
    monkeypatch.setattr(
        uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None)
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", _extract_archive_base_row)
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_first_order_code",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_page_number",
        lambda _p: asyncio.sleep(0, result=page.index + 1),
    )
    monkeypatch.setattr(
        uc_main,
        "_is_button_disabled",
        lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert extract.page_count == 2
    assert len(extract.base_rows) == 47
    assert "O048" not in {row["order_code"] for row in extract.base_rows}


@pytest.mark.asyncio
async def test_collect_archive_orders_aborts_after_stalled_next_click_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 48),
            "next_disabled": False,
            "stall_next_clicks": 2,
        },
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]),
    )
    monkeypatch.setattr(
        uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None)
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_first_order_code",
        lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_page_number",
        lambda _p: asyncio.sleep(0, result=page.index + 1),
    )
    monkeypatch.setattr(
        uc_main,
        "_is_button_disabled",
        lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert len(extract.base_rows) == 30
    assert extract.page_count == 1
    assert (
        extract.skipped_order_counters[
            "partial_extraction_non_advancing_next_click_after_retry"
        ]
        == 1
    )
    assert extract.skipped_order_counters["footer_baseline_unavailable"] == 1


def test_archive_extraction_gap_uses_footer_total() -> None:
    extract = uc_main.ArchiveOrdersExtract(
        base_rows=[{"order_code": "O1"}], post_filter_footer_total=3
    )
    assert uc_main._archive_extraction_gap(extract) == 2


def test_summary_overall_status_rolls_up_partial_reason_codes() -> None:
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-1",
        run_env="test",
        report_date=uc_main.date(2025, 1, 1),
        report_end_date=uc_main.date(2025, 1, 1),
        store_codes=["UCX"],
    )
    summary.record_store(
        "UCX",
        uc_main.StoreOutcome(
            status="ok",
            message="Archive Orders download complete",
            reason_codes=["partial_extraction"],
        ),
    )

    assert summary.overall_status() == "partial"


class _FakeSimpleLocator:
    def __init__(
        self,
        text: str | None = None,
        present: bool = True,
        children: dict[str, "_FakeSimpleLocator"] | None = None,
    ):
        self._text = text
        self._present = present
        self._children = children or {}

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return 1 if self._present else 0

    async def text_content(self, timeout: int | None = None) -> str | None:
        return self._text

    async def inner_text(self, timeout: int | None = None) -> str:
        return self._text or ""

    def locator(self, selector: str):
        return self._children.get(selector, _FakeSimpleLocator(present=False))


class _FakeRowForOrderCode:
    def __init__(self, mapping: dict[str, _FakeSimpleLocator]):
        self._mapping = mapping

    def locator(self, selector: str):
        return self._mapping.get(selector, _FakeSimpleLocator(present=False))


@pytest.mark.asyncio
async def test_get_archive_order_code_falls_back_to_anchor_when_primary_missing() -> (
    None
):
    row = _FakeRowForOrderCode(
        {
            "td.order-col span[style*='cursor']": _FakeSimpleLocator(present=False),
            "td.order-col span a": _FakeSimpleLocator(text="  UC-456  "),
        }
    )

    order_code = await uc_main._get_archive_order_code(row)

    assert order_code == "UC-456"


@pytest.mark.asyncio
async def test_get_archive_order_code_falls_back_to_nested_button_text() -> None:
    nested_button = _FakeSimpleLocator(text=" UC-789 ")
    order_col = _FakeSimpleLocator(children={"button": nested_button})
    row = _FakeRowForOrderCode(
        {
            "td.order-col span[style*='cursor']": _FakeSimpleLocator(present=False),
            "td.order-col": order_col,
        }
    )

    order_code = await uc_main._get_archive_order_code(row)

    assert order_code == "UC-789"


@pytest.mark.asyncio
async def test_wait_for_archive_page_stability_footer_changes_then_stabilizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StabilityPage:
        def __init__(self):
            self.timeouts: list[int] = []

        async def wait_for_timeout(self, ms: int):
            self.timeouts.append(ms)

    page = _StabilityPage()
    footer_samples = iter([(1, 30, 100), (31, 60, 100), (31, 60, 100)])
    signature_samples = iter([("O1",), ("O31",), ("O31",)])

    async def _footer(_page):
        return next(footer_samples)

    async def _signatures(_page):
        return next(signature_samples)

    monkeypatch.setattr(uc_main, "_get_archive_footer_window", _footer)
    monkeypatch.setattr(uc_main, "_get_archive_row_signatures", _signatures)
    state = await uc_main._wait_for_archive_page_stability(
        page=page, timeout_ms=2_000, sample_interval_ms=1
    )

    assert state.stable is True
    assert state.stable_footer_window == (31, 60, 100)
    assert state.stability_attempts == 3


@pytest.mark.asyncio
async def test_wait_for_archive_page_stability_signatures_prevent_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StabilityPage:
        async def wait_for_timeout(self, _ms: int):
            return None

    page = _StabilityPage()
    footer_samples = iter([(1, 30, 100), (1, 30, 100), (1, 30, 100), (1, 30, 100)])
    signature_samples = iter([("O1",), ("O2",), ("O2",), ("O2",)])

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=next(footer_samples)),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_row_signatures",
        lambda _p: asyncio.sleep(0, result=next(signature_samples)),
    )

    state = await uc_main._wait_for_archive_page_stability(
        page=page, timeout_ms=2_000, sample_interval_ms=1
    )

    assert state.stable is True
    assert state.stability_attempts == 3


@pytest.mark.asyncio
async def test_wait_for_archive_page_stability_timeout_returns_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StabilityPage:
        async def wait_for_timeout(self, _ms: int):
            return None

    page = _StabilityPage()
    footer_samples = iter([(1, 30, 100), (31, 60, 100), (61, 90, 100)])
    signature_samples = iter([("O1",), ("O31",), ("O61",)])

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_window",
        lambda _p: asyncio.sleep(0, result=next(footer_samples, (61, 90, 100))),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_row_signatures",
        lambda _p: asyncio.sleep(0, result=next(signature_samples, ("O61",))),
    )

    state = await uc_main._wait_for_archive_page_stability(
        page=page, timeout_ms=0, sample_interval_ms=1
    )

    assert state.stable is False
    assert state.last_observed_footer_window is not None
    assert state.last_observed_signature_hash is not None


@pytest.mark.asyncio
async def test_collect_archive_orders_warns_and_exits_when_stability_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 48),
            "next_disabled": False,
        }
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_page_stability",
        lambda **kwargs: asyncio.sleep(
            0,
            result=uc_main.ArchivePageStabilityState(
                stable=False,
                stability_attempts=5,
                stable_footer_window=None,
                stable_signature_hash=None,
                last_observed_footer_window=(1, 30, 48),
                last_observed_signature_hash="abc",
                stability_timeout_ms=1_000,
            ),
        ),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert extract.page_count == 1
    assert (
        extract.skipped_order_counters["stability_timeout_before_pagination_decision"]
        == 1
    )


@pytest.mark.asyncio
async def test_collect_archive_orders_stable_footer_completion_does_not_click_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 30),
            "next_disabled": False,
        }
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_page_stability",
        lambda **kwargs: asyncio.sleep(
            0,
            result=uc_main.ArchivePageStabilityState(
                stable=True,
                stability_attempts=2,
                stable_footer_window=(1, 30, 30),
                stable_signature_hash="sig",
                last_observed_footer_window=(1, 30, 30),
                last_observed_signature_hash="sig",
                stability_timeout_ms=1_000,
            ),
        ),
    )

    extract = await uc_main._collect_archive_orders(
        page=page, store=store, logger=logger
    )

    assert extract.page_count == 1
    assert page.index == 0


def test_partial_reason_uses_post_filter_baseline_not_pre_filter() -> None:
    extract = uc_main.ArchiveOrdersExtract(
        base_rows=[{"order_code": f"O{i}"} for i in range(1, 11)],
        pre_filter_footer_total=100,
        post_filter_footer_total=10,
        footer_baseline_source="post_filter_refresh",
        footer_baseline_stable=True,
    )

    assert uc_main._archive_extraction_gap(extract) == 0


def test_store_outcome_marks_partial_when_extracted_below_post_filter_total() -> None:
    outcome = uc_main.StoreOutcome(
        status="warning",
        message="partial",
        reason_codes=["partial_extraction"],
        post_filter_footer_total=10,
        base_rows_extracted=8,
    )

    status = uc_main._resolve_sync_log_status(
        outcome=outcome, download_succeeded=True, row_count=8
    )

    assert status == "partial"


def test_store_outcome_marks_warning_when_footer_baseline_unavailable() -> None:
    outcome = uc_main.StoreOutcome(
        status="warning",
        message="baseline unavailable",
        reason_codes=["footer_baseline_unavailable"],
        footer_baseline_source="fallback_unknown",
        footer_baseline_stable=False,
    )

    status = uc_main._resolve_sync_log_status(
        outcome=outcome, download_succeeded=True, row_count=3
    )

    assert status == "success_with_warnings"


def test_summary_overall_status_rolls_up_publish_preflight_warning() -> None:
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-publish-warning",
        run_env="test",
        report_date=uc_main.date(2025, 1, 1),
        report_end_date=uc_main.date(2025, 1, 1),
        store_codes=["UCX"],
    )
    summary.record_store(
        "UCX",
        uc_main.StoreOutcome(
            status="ok",
            message="Archive Orders extracted 10 rows",
            archive_publish_sales={
                "warnings": 1,
                "skipped": 10,
                "reason_codes": {"preflight_parent_coverage_near_zero": 1},
            },
        ),
    )

    assert summary.overall_status() == "success_with_warnings"
    payload = summary._build_notification_payload(
        finished_at=summary.started_at,
        total_time_taken="00:00:00",
    )
    assert payload["overall_status"] == "success_with_warnings"
    assert payload["stores"][0]["status"] == "success_with_warnings"


def test_store_outcome_marks_warning_for_high_publish_skips_with_warnings() -> None:
    outcome = uc_main.StoreOutcome(
        status="ok",
        message="Archive Orders extracted 10 rows",
        archive_publish_orders={
            "warnings": 6,
            "skipped": 6,
            "reason_codes": {"missing_parent_order_context": 6},
        },
    )

    status = uc_main._resolve_sync_log_status(
        outcome=outcome, download_succeeded=True, row_count=10
    )

    assert status == "success_with_warnings"


class _FakeFilterLocator:
    def __init__(self, *, present: bool = True, count_value: int = 1):
        self._present = present
        self._count_value = count_value

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return self._count_value if self._present else 0

    async def click(self, *args, **kwargs) -> None:
        return None

    async def check(self) -> None:
        return None

    async def fill(self, _value: str) -> None:
        return None

    def nth(self, _idx: int):
        return self

    async def wait_for(self, *args, **kwargs) -> None:
        return None


class _FakeFilterPage:
    def __init__(self):
        self.url = "https://store.ucleanlaundry.com/archive"

    def locator(self, selector: str):
        if selector == uc_main.ARCHIVE_CUSTOM_INPUT_SELECTOR:
            return _FakeFilterLocator(present=True, count_value=2)
        return _FakeFilterLocator(present=True)

    async def wait_for_selector(self, *args, **kwargs) -> None:
        return None


@pytest.mark.asyncio
async def test_apply_archive_date_filter_logs_baseline_and_stable_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    page = _FakeFilterPage()
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )

    monkeypatch.setattr(
        uc_main,
        "_get_archive_footer_text",
        lambda _p: asyncio.sleep(0, result="Showing results 1 to 10 of 10 total"),
    )
    monkeypatch.setattr(
        uc_main,
        "_get_archive_row_signatures",
        lambda _p: asyncio.sleep(0, result=("O001", "O002")),
    )
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_filter_refresh_completion",
        lambda **kwargs: asyncio.sleep(
            0,
            result=uc_main.ArchiveFilterRefreshState(
                post_filter_footer_window=(1, 10, 10),
                footer_during_refresh=[
                    {
                        "elapsed_ms": 50,
                        "raw_text": "Showing results 1 to 10 of 10 total",
                        "parsed_window": (1, 10, 10),
                    }
                ],
                table_rows_during_refresh=[{"elapsed_ms": 50, "row_count": 2}],
                refresh_phase_marker="stable",
                refresh_attempts=2,
                refresh_elapsed_ms=100,
            ),
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_page_stability",
        lambda **kwargs: asyncio.sleep(
            0,
            result=uc_main.ArchivePageStabilityState(
                stable=True,
                stability_attempts=2,
                stable_footer_window=(1, 10, 10),
                stable_signature_hash="sig",
                last_observed_footer_window=(1, 10, 10),
                last_observed_signature_hash="sig",
                stability_timeout_ms=1_000,
            ),
        ),
    )

    state = await uc_main._apply_archive_date_filter(
        page=page,
        store=store,
        logger=logger,
        from_date=uc_main.date(2025, 1, 1),
        to_date=uc_main.date(2025, 1, 31),
    )

    logs = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    baseline_log = next(
        log
        for log in logs
        if log.get("message") == "Archive Orders filter baseline captured"
    )
    stable_log = next(
        log
        for log in logs
        if log.get("message") == "Archive Orders filter refresh stabilized snapshot"
    )

    assert baseline_log["footer_before_filter"]["raw_text"]
    assert baseline_log["table_rows_before_filter"] == 2
    assert stable_log["footer_after_filter_stable"]["parsed_window"] == [1, 10, 10]
    assert stable_log["table_rows_after_filter_stable"] == 2
    assert stable_log["selected_from_date"] == "2025-01-01"
    assert stable_log["selected_to_date"] == "2025-01-31"
    assert state is not None
    assert state.footer_baseline_source == "post_filter_refresh"


@pytest.mark.asyncio
async def test_collect_archive_orders_logs_first_page_baseline_and_drift_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "order_codes": [f"O{i:03d}" for i in range(1, 31)],
            "footer": (1, 30, 30),
            "next_disabled": True,
        }
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(
        store_code="UCX", store_name=None, cost_center=None, sync_config={}
    )
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)

    monkeypatch.setattr(
        uc_main,
        "_get_archive_order_code",
        lambda row: asyncio.sleep(0, result=row.order_code),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_archive_base_row",
        lambda **kwargs: asyncio.sleep(
            0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_order_details",
        lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)),
    )
    monkeypatch.setattr(
        uc_main,
        "_extract_payment_details",
        lambda **kwargs: asyncio.sleep(0, result=[]),
    )
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_page_stability",
        lambda **kwargs: asyncio.sleep(
            0,
            result=uc_main.ArchivePageStabilityState(
                stable=True,
                stability_attempts=2,
                stable_footer_window=(1, 30, 30),
                stable_signature_hash="sig",
                last_observed_footer_window=(1, 30, 30),
                last_observed_signature_hash="sig",
                stability_timeout_ms=1_000,
            ),
        ),
    )
    monkeypatch.setattr(
        uc_main, "_is_button_disabled", lambda _b: asyncio.sleep(0, result=True)
    )

    await uc_main._collect_archive_orders(
        page=page,
        store=store,
        logger=logger,
        post_filter_footer_window=(1, 20, 30),
        post_filter_footer_total=30,
        footer_baseline_source="post_filter_refresh",
        footer_baseline_stable=True,
    )

    logs = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    first_page_log = next(
        log for log in logs if log.get("message") == "Archive Orders page loaded"
    )
    drift_log = next(
        log
        for log in logs
        if log.get("reason_code") == "footer_baseline_drift_after_refresh"
    )

    assert first_page_log["footer_baseline_source"] == "post_filter_refresh"
    assert first_page_log["baseline_footer_window"] == [1, 20, 30]
    assert first_page_log["current_footer_window"] == [1, 30, 30]
    assert drift_log["baseline_footer_window"] == [1, 20, 30]
    assert drift_log["current_footer_window"] == [1, 30, 30]
