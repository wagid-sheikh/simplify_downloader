import argparse
import asyncio
import io

import pytest

from app.dashboard_downloader import cli
from app.dashboard_downloader import pipeline as pipeline_module
from app.dashboard_downloader import settings as settings_module
from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.settings import PipelineSettings


def test_run_async_loads_settings_without_overrides(monkeypatch):
    observed: dict[str, object] = {}

    async def fake_load_settings(*, dry_run: bool, run_id: str) -> PipelineSettings:
        observed["params"] = {
            "dry_run": dry_run,
            "run_id": run_id,
        }
        return PipelineSettings(
            run_id=run_id,
            stores={"A100": {}},
            raw_store_env="store_master.etl_flag",
            dry_run=dry_run,
        )

    async def fake_run_pipeline(*, settings: PipelineSettings, logger, aggregator):
        observed["settings"] = settings
        observed["logger"] = logger
        observed["aggregator"] = aggregator

    monkeypatch.setattr(settings_module, "load_settings", fake_load_settings)
    monkeypatch.setattr(pipeline_module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(
        cli,
        "get_logger",
        lambda run_id: JsonLogger(run_id=run_id, stream=io.StringIO(), log_file_path=None),
    )

    args = argparse.Namespace(dry_run=False, run_id="run-test", run_migrations=False)

    result = asyncio.run(cli._run_async(args))

    assert result == 0
    assert observed["params"] == {
        "dry_run": False,
        "run_id": "run-test",
    }
    assert observed["settings"].raw_store_env == "store_master.etl_flag"
    assert observed["aggregator"].store_codes == ["A100"]


def test_main_rejects_store_override_arguments():
    with pytest.raises(SystemExit):
        cli.main(["run", "--stores_list", "A100"])

    with pytest.raises(SystemExit):
        cli.main(["run-single-session", "--stores_list", "A100"])


def test_run_async_invokes_leads_assignment_on_success(monkeypatch):
    observed: dict[str, object] = {"lead_calls": 0}

    async def fake_load_settings(*, dry_run: bool, run_id: str) -> PipelineSettings:
        return PipelineSettings(
            run_id=run_id,
            stores={"A100": {}},
            raw_store_env="store_master.etl_flag",
            dry_run=dry_run,
        )

    async def fake_run_pipeline(*, settings: PipelineSettings, logger, aggregator):
        return None

    async def fake_leads_assignment(*, env: str, run_id: str):
        observed["lead_calls"] = int(observed["lead_calls"]) + 1
        observed["env"] = env
        observed["run_id"] = run_id

    monkeypatch.setattr(settings_module, "load_settings", fake_load_settings)
    monkeypatch.setattr(pipeline_module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(
        cli,
        "get_logger",
        lambda run_id: JsonLogger(run_id=run_id, stream=io.StringIO(), log_file_path=None),
    )

    import sys
    import types

    lead_module = types.SimpleNamespace(run_leads_assignment_pipeline=fake_leads_assignment)
    monkeypatch.setitem(sys.modules, "app.lead_assignment.pipeline", lead_module)


    args = argparse.Namespace(dry_run=False, run_id="run-success", run_migrations=False)
    result = asyncio.run(cli._run_async(args))

    assert result == 0
    assert observed["lead_calls"] == 1
    assert observed["run_id"] == "run-success"
