import importlib
import sys
import types

import pytest


@pytest.fixture()
def run_store_reports_module(tmp_path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    fake_config_module = types.ModuleType("app.config")
    fake_config_module.config = types.SimpleNamespace(
        reports_root=str(reports_dir),
        database_url="sqlite://",
        td_base_url="https://example.com",
        td_login_url="https://example.com/login",
        td_home_url="https://example.com/home",
        tms_base="https://example.com",
        td_store_dashboard_path="/dash/{store_code}",
        td_storage_state_filename="storage_state.json",
        td_global_username="user",
        td_global_password="pass",
    )
    original_config = sys.modules.get("app.config")
    sys.modules["app.config"] = fake_config_module

    module = importlib.import_module("app.dashboard_downloader.run_store_reports")
    importlib.reload(module)

    yield module

    sys.modules.pop("app.dashboard_downloader.run_store_reports", None)
    if original_config is not None:
        sys.modules["app.config"] = original_config
    else:
        sys.modules.pop("app.config", None)


def test_resolve_template_file_accepts_directory(tmp_path, run_store_reports_module):
    template_dir = tmp_path / "templates"
    template_dir.mkdir()
    template_file = template_dir / run_store_reports_module.STORE_TEMPLATE_FILE_NAME
    template_file.write_text("<html></html>")

    resolved = run_store_reports_module._resolve_template_file(template_dir)

    assert resolved == template_file


def test_resolve_template_file_accepts_file(tmp_path, run_store_reports_module):
    template_file = tmp_path / "custom_template.html"
    template_file.write_text("<html></html>")

    resolved = run_store_reports_module._resolve_template_file(template_file)

    assert resolved == template_file


def test_resolve_template_file_missing(tmp_path, run_store_reports_module):
    missing_path = tmp_path / "does_not_exist.html"

    with pytest.raises(FileNotFoundError):
        run_store_reports_module._resolve_template_file(missing_path)


def _diagnostics(module, *, etl_codes=(), report_codes=(), eligible_codes=()):
    return module.StoreScopeDiagnostics(
        etl_enabled_codes=list(etl_codes),
        report_enabled_codes=list(report_codes),
        report_eligible_codes=list(eligible_codes),
    )


def test_store_scope_warning_for_zero_report_enabled_stores(monkeypatch, run_store_reports_module):
    events = []
    monkeypatch.setattr(run_store_reports_module, "log_event", lambda **kwargs: events.append(kwargs))

    run_store_reports_module._log_store_scope_diagnostics(
        logger=object(),
        diagnostics=_diagnostics(run_store_reports_module),
    )

    assert events == [
        {
            "logger": events[0]["logger"],
            "phase": "report",
            "status": "warning",
            "message": "no report-eligible stores found in store_master, skipping report generation",
            "extras": {
                "etl_enabled_count": 0,
                "report_enabled_count": 0,
                "report_eligible_count": 0,
                "etl_enabled_codes": [],
                "report_enabled_codes": [],
                "report_eligible_codes": [],
            },
        }
    ]


def test_store_scope_warning_for_etl_enabled_but_report_disabled_stores(
    monkeypatch, run_store_reports_module
):
    events = []
    monkeypatch.setattr(run_store_reports_module, "log_event", lambda **kwargs: events.append(kwargs))

    run_store_reports_module._log_store_scope_diagnostics(
        logger=object(),
        diagnostics=_diagnostics(run_store_reports_module, etl_codes=["A100", "B200"]),
    )

    assert events[0]["status"] == "warning"
    assert events[0]["extras"] == {
        "etl_enabled_count": 2,
        "report_enabled_count": 0,
        "report_eligible_count": 0,
        "etl_enabled_codes": ["A100", "B200"],
        "report_enabled_codes": [],
        "report_eligible_codes": [],
    }


def test_store_scope_notice_for_normal_report_enabled_scope(monkeypatch, run_store_reports_module):
    events = []
    monkeypatch.setattr(run_store_reports_module, "log_event", lambda **kwargs: events.append(kwargs))

    run_store_reports_module._log_store_scope_diagnostics(
        logger=object(),
        diagnostics=_diagnostics(
            run_store_reports_module,
            etl_codes=["A100", "B200"],
            report_codes=["B200"],
            eligible_codes=["B200"],
        ),
    )

    assert events[0]["status"] == "info"
    assert events[0]["message"] == "dashboard store scope diagnostics"
    assert events[0]["extras"]["report_eligible_count"] == 1
    assert events[0]["extras"]["report_eligible_codes"] == ["B200"]


@pytest.mark.asyncio
async def test_resolve_store_codes_requires_both_etl_and_report_flags(
    monkeypatch, run_store_reports_module
):
    observed = {}

    async def fake_fetch_store_codes(**kwargs):
        observed.update(kwargs)
        return ["A100"]

    monkeypatch.setattr(run_store_reports_module, "fetch_store_codes", fake_fetch_store_codes)

    resolved = await run_store_reports_module._resolve_store_codes(
        database_url="postgresql://example",
        store_codes=None,
        logger=object(),
    )

    assert resolved == ["A100"]
    assert observed == {
        "database_url": "postgresql://example",
        "etl_flag": True,
        "report_flag": True,
        "store_codes": None,
    }


@pytest.mark.asyncio
async def test_zero_report_eligible_scope_skips_generation_without_failing(
    monkeypatch, run_store_reports_module
):
    events = []

    async def fake_fetch_store_scope_diagnostics(*, database_url):
        assert database_url == "postgresql://example"
        return _diagnostics(run_store_reports_module, etl_codes=["A100"])

    async def fake_resolve_store_codes(**kwargs):
        return []

    monkeypatch.setattr(
        run_store_reports_module,
        "fetch_store_scope_diagnostics",
        fake_fetch_store_scope_diagnostics,
    )
    monkeypatch.setattr(run_store_reports_module, "_resolve_store_codes", fake_resolve_store_codes)
    monkeypatch.setattr(run_store_reports_module, "log_event", lambda **kwargs: events.append(kwargs))

    generated = await run_store_reports_module.run_store_reports_for_date(
        run_store_reports_module.date(2026, 5, 29),
        logger=object(),
        run_id="run-1",
        database_url="postgresql://example",
    )

    assert generated == []
    assert events[0]["status"] == "warning"
    assert events[0]["extras"]["etl_enabled_count"] == 1
    assert events[0]["extras"]["report_eligible_count"] == 0
