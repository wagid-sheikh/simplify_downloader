import pytest

from app.dashboard_downloader import settings


@pytest.mark.asyncio
async def test_load_settings_uses_etl_flag(monkeypatch):
    async def fake_fetch_store_codes(*, database_url, etl_flag=None, report_flag=None, store_codes=None):
        if etl_flag:
            return ["A100", "B200"]
        if report_flag:
            return ["A100"]
        return []

    monkeypatch.setattr(settings, "fetch_store_codes", fake_fetch_store_codes)

    loaded = await settings.load_settings(dry_run=False, run_id="run-1")

    assert set(loaded.stores.keys()) == {"A100", "B200"}
    assert loaded.raw_store_env == "store_master.etl_flag"


@pytest.mark.asyncio
async def test_load_settings_requires_report_alignment(monkeypatch):
    async def fake_fetch_store_codes(*, database_url, etl_flag=None, report_flag=None, store_codes=None):
        if etl_flag:
            return ["A100"]
        if report_flag:
            return ["A100", "B200"]
        return []

    monkeypatch.setattr(settings, "fetch_store_codes", fake_fetch_store_codes)

    with pytest.raises(ValueError, match="Report-eligible stores are missing"):
        await settings.load_settings(dry_run=False, run_id="run-1")


@pytest.mark.asyncio
async def test_load_settings_requires_etl_flagged_stores(monkeypatch):
    async def fake_fetch_store_codes(*, database_url, etl_flag=None, report_flag=None, store_codes=None):
        if etl_flag:
            return []
        if report_flag:
            return ["A100"]
        return []

    monkeypatch.setattr(settings, "fetch_store_codes", fake_fetch_store_codes)

    with pytest.raises(ValueError, match="No stores are flagged for ETL"):
        await settings.load_settings(dry_run=False, run_id="run-1")


@pytest.mark.asyncio
async def test_fetch_store_scope_diagnostics_groups_active_db_store_codes(monkeypatch):
    from contextlib import asynccontextmanager

    from app.dashboard_downloader import config as dashboard_config

    class FakeSession:
        async def execute(self, statement):
            return [
                ("A100", True, False),
                ("b200", True, True),
                ("C300", False, True),
                ("B200", True, True),
            ]

    @asynccontextmanager
    async def fake_session_scope(database_url):
        assert database_url == "postgresql://example"
        yield FakeSession()

    monkeypatch.setattr(dashboard_config, "session_scope", fake_session_scope)

    diagnostics = await dashboard_config.fetch_store_scope_diagnostics(
        database_url="postgresql://example"
    )

    assert diagnostics.as_dict() == {
        "etl_enabled_count": 2,
        "report_enabled_count": 2,
        "report_eligible_count": 1,
        "etl_enabled_codes": ["A100", "B200"],
        "report_enabled_codes": ["B200", "C300"],
        "report_eligible_codes": ["B200"],
    }
