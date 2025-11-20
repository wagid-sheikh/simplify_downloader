import pytest

from app.dashboard_downloader import settings


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
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


@pytest.mark.anyio("asyncio")
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


@pytest.mark.anyio("asyncio")
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
