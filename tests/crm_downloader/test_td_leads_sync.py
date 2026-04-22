from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from app.crm_downloader.td_leads_sync.ingest import build_lead_uid
from app.crm_downloader.td_leads_sync import ingest as td_leads_ingest
from app.crm_downloader.td_leads_sync.main import (
    LeadsRunSummary,
    StoreLeadResult,
    _build_td_leads_summary_html,
    _build_td_leads_tables_html,
    _available_pager_args,
    _ensure_scheduler_page,
    _field_from_headers,
    _find_tz_aware_columns,
    _postback_page_arg,
    _sanitize_rows_for_xlsx_export,
    _scrape_grid_rows,
    _write_store_artifact,
)
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.config import config as app_config


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


def test_run_summary_record_includes_duration_for_failed_store_runs() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 2, 30, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-1",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={
            "A668": StoreLeadResult(store_code="A668", status="error", message="store timed out"),
        },
    )

    record = summary.build_record(finished_at=finished_at)

    assert record["overall_status"] == "failed"
    assert record["total_time_taken"] == "00:02:30"
    assert record["metrics_json"]["duration_seconds"] == 150
    assert record["metrics_json"]["duration_human"] == "00:02:30"


def test_td_leads_summary_html_renders_compact_summary_tables_and_footer_refs() -> None:
    summary = LeadsRunSummary(
        run_id="run-1",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "customer_name": "Nia",
                        "mobile": "9000000000",
                        "pickup_id": "P-10",
                        "pickup_date": "2026-04-22T10:15:00+00:00",
                        "status_text": "Pending",
                    },
                    {
                        "status_bucket": "completed",
                        "customer_name": "Raj",
                        "mobile": "9111111111",
                        "pickup_id": "C-2",
                        "pickup_date": "2026-04-22T09:00:00+00:00",
                        "status_text": "Completed",
                    },
                ],
                status_counts={"pending": 1, "completed": 1, "cancelled": 0},
                ingested_rows=2,
                artifact_path="app/crm_downloader/data/A817-crm_leads.xlsx",
            )
        },
    )

    summary_html = _build_td_leads_summary_html(summary=summary, duration_human="00:01:00")

    assert "Per-store summary" in summary_html
    assert "Status bucket totals" in summary_html
    assert "A817" in summary_html
    assert "Total stores processed:</strong> 1" in summary_html
    assert "Runtime duration:</strong> 00:01:00" in summary_html
    assert "Reference run_id: <code>run-1</code>" in summary_html


def test_td_leads_run_summary_record_exposes_summary_html_in_metrics() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 1, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-4",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={
            "A668": StoreLeadResult(
                store_code="A668",
                rows=[{"status_bucket": "pending", "customer_name": "Ada", "pickup_date": "2026-04-22"}],
            )
        },
    )

    record = summary.build_record(finished_at=finished_at)

    assert "Total Stores Processed: 1" in record["summary_text"]
    assert "status=pending, customer_name=Ada" not in record["summary_text"]
    assert "Per-store summary" in record["metrics_json"]["summary_html"]
    assert "Row-level lead tables" in record["metrics_json"]["summary_html"]
    assert "Store A668" in record["metrics_json"]["lead_tables_html"]
    assert "A668" in record["metrics_json"]["summary_html"]
    assert "rows" not in record["metrics_json"]["stores"][0]


def test_td_leads_tables_html_renders_store_sections_bucket_tables_and_rows() -> None:
    pending_rows = [
        {
            "status_bucket": "pending",
            "pickup_id": f"P-{index}",
            "customer_name": f"Pending {index}",
            "mobile": "9000000000",
            "address": "Area 1",
            "pickup_date": "2026-04-22 10:15",
            "status_text": "Urgent",
        }
        for index in range(1, 53)
    ]
    summary = LeadsRunSummary(
        run_id="run-html",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    *pending_rows,
                    {
                        "status_bucket": "completed",
                        "pickup_id": "C-2",
                        "customer_name": "Raj",
                        "mobile": "9111111111",
                        "address": "Area 2",
                        "pickup_date": "2026-04-22 09:00",
                        "status_text": "Done",
                    },
                ],
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary, row_limit=50)

    assert "Store A817" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>Pending</h5>" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>Completed</h5>" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>Cancelled</h5>" in tables_html
    assert "Pending 1" in tables_html
    assert "Raj" in tables_html
    assert "No cancelled leads." in tables_html
    assert "Pending 52" not in tables_html
    assert "+2 more rows in artifact for A817 pending." in tables_html


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


@pytest.mark.asyncio
async def test_ingest_store_path_uses_async_session_scope_without_greenlet_errors(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads.db'}"

    result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[
            {
                "store_code": "A668",
                "status_bucket": "pending",
                "pickup_id": "4434944",
                "pickup_no": "A668-3025",
                "mobile": "9599242207",
                "pickup_date": "22 Apr 2026",
                "pickup_time": "11:00 AM - 1:00 PM",
            }
        ],
        run_id="run-1",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert result.rows_received == 1
    assert result.rows_upserted == 1

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            count = await connection.scalar(sa.text("SELECT COUNT(*) FROM crm_leads"))
        assert count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_td_leads_seeded_run_notification_plans_email(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_notif.db'}"
    engine = create_async_engine(database_url, future=True)

    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE pipelines (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT NOT NULL,
                        description TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE pipeline_run_summaries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_name TEXT,
                        run_id TEXT,
                        run_env TEXT,
                        started_at DATETIME,
                        finished_at DATETIME,
                        total_time_taken TEXT,
                        report_date DATE,
                        overall_status TEXT,
                        summary_text TEXT,
                        phases_json JSON,
                        metrics_json JSON,
                        created_at DATETIME
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE documents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        doc_type TEXT,
                        doc_subtype TEXT,
                        doc_date DATE,
                        reference_name_1 TEXT,
                        reference_id_1 TEXT,
                        reference_name_2 TEXT,
                        reference_id_2 TEXT,
                        reference_name_3 TEXT,
                        reference_id_3 TEXT,
                        file_name TEXT,
                        mime_type TEXT,
                        file_size_bytes INTEGER,
                        storage_backend TEXT,
                        file_path TEXT,
                        file_blob BLOB,
                        checksum TEXT,
                        status TEXT,
                        error_message TEXT,
                        created_at DATETIME,
                        created_by TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE notification_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id INTEGER,
                        code TEXT,
                        description TEXT,
                        env TEXT,
                        scope TEXT,
                        attach_mode TEXT,
                        is_active BOOLEAN
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE email_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id INTEGER,
                        name TEXT,
                        subject_template TEXT,
                        body_template TEXT,
                        is_active BOOLEAN
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE notification_recipients (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id INTEGER,
                        store_code TEXT,
                        env TEXT,
                        email_address TEXT,
                        display_name TEXT,
                        send_as TEXT,
                        is_active BOOLEAN,
                        created_at DATETIME
                    )
                    """
                )
            )
            await connection.execute(
                sa.text("INSERT INTO pipelines (id, code, description) VALUES (1, 'td_crm_leads_sync', 'TD leads')")
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO pipeline_run_summaries (
                        pipeline_name, run_id, run_env, started_at, finished_at, report_date, overall_status,
                        total_time_taken, summary_text, metrics_json
                    ) VALUES (
                        'td_crm_leads_sync', 'run-1', 'local', '2026-04-22T00:00:00+00:00', '2026-04-22T00:01:00+00:00',
                        '2026-04-22', 'success', '00:01:00', 'ok',
                        :metrics_json
                    )
                    """
                ),
                {
                    "metrics_json": json.dumps({"summary_html": "<div>ok</div>"}),
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO notification_profiles (
                        id, pipeline_id, code, description, env, scope, attach_mode, is_active
                    ) VALUES (
                        10, 1, 'run_summary', 'TD leads run summary', 'any', 'run', 'none', 1
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO email_templates (
                        profile_id, name, subject_template, body_template, is_active
                    ) VALUES (
                        10, 'run_summary', 'TD Leads {{ run_id }}', 'Run {{ run_id }} complete in {{ duration_human }}', 1
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO notification_recipients (
                        profile_id, store_code, env, email_address, send_as, is_active
                    ) VALUES (
                        10, 'ALL', 'any', 'ops@example.com', 'to', 1
                    )
                    """
                )
            )

        monkeypatch.setattr(
            "app.dashboard_downloader.notifications.config",
            SimpleNamespace(
                database_url=database_url,
                report_email_smtp_host=app_config.report_email_smtp_host,
                report_email_smtp_port=app_config.report_email_smtp_port,
                report_email_from=app_config.report_email_from,
                report_email_smtp_username=app_config.report_email_smtp_username,
                report_email_smtp_password=app_config.report_email_smtp_password,
                report_email_use_tls=app_config.report_email_use_tls,
            ),
        )
        sent_plans = []

        def _capture_send_email(_smtp_config, plan):
            sent_plans.append(plan)
            return True

        monkeypatch.setattr("app.dashboard_downloader.notifications._send_email", _capture_send_email)

        result = await send_notifications_for_run("td_crm_leads_sync", "run-1")

        assert result["emails_planned"] == 1
        assert result["emails_sent"] == 1
        assert len(sent_plans) == 1
        assert "00:01:00" in sent_plans[0].body
        assert "Run run-1 complete in 00:01:00" in sent_plans[0].body
    finally:
        await engine.dispose()
