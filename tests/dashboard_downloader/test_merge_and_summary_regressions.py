from datetime import datetime, timezone
from pathlib import Path

from app.dashboard_downloader import run_downloads
from app.dashboard_downloader.run_summary import RunAggregator


def test_manual_merge_bucket_skips_no_data_sentinel(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_downloads, "DATA_DIR", tmp_path)
    monkeypatch.setattr(run_downloads, "MERGED_NAMES", {"nonpackage_all": "merged_nonpackage_all_test.csv"})

    no_data_file = tmp_path / "A012-non-package-all.csv"
    no_data_file.write_text("No data available to export.", encoding="utf-8")

    valid_file = tmp_path / "A040-non-package-all.csv"
    valid_file.write_text(
        "Store Code,Store Name,Mobile No.,Taxable Amount,Order Date,Expected Delivery Date,Actual Delivery Date\n"
        "A040,Store A,7011717793,1345.00,2026-03-03 18:05:38,2026-03-07,N/A\n",
        encoding="utf-8",
    )

    merged = run_downloads._manual_merge_bucket("nonpackage_all", [no_data_file, valid_file])

    assert merged is not None
    assert merged.name == "merged_nonpackage_all_test.csv"
    assert run_downloads._count_rows(merged) == 1
    merged_text = merged.read_text(encoding="utf-8")
    assert merged_text.startswith("Store Code,Store Name,Mobile No.")
    assert "No data available to export." not in merged_text


def test_run_summary_missed_leads_uses_ingested_by_store() -> None:
    aggregator = RunAggregator(
        run_id="run-1",
        run_env="prod",
        store_codes=["A012"],
    )
    aggregator.set_report_date(datetime(2026, 3, 22, tzinfo=timezone.utc).date())
    aggregator.record_download_summary({"missed_leads": {"A012": {"rows": 86}}})
    aggregator.record_bucket_counts(
        "missed_leads",
        {
            "download_total": 86,
            "merged_rows": 86,
            "ingested_rows": 10,
            "ingested_by_store": {"A012": 10},
        },
    )

    summary_text = aggregator.build_summary_text(
        finished_at=datetime(2026, 3, 23, tzinfo=timezone.utc)
    )

    assert "A012: downloaded 86, ingested 10" in summary_text

import io

import pytest

from app.dashboard_downloader import pipeline as dashboard_pipeline
from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.settings import PipelineSettings


@pytest.mark.asyncio
async def test_run_pipeline_ignores_int_bucket_payload(monkeypatch) -> None:
    settings = PipelineSettings(
        run_id="run-int-payload",
        stores={"A100": {"store_name": "Store A100"}},
        raw_store_env="store_master.etl_flag",
        dry_run=True,
    )
    logger = JsonLogger(run_id=settings.run_id, stream=io.StringIO(), log_file_path=None)
    aggregator = dashboard_pipeline.RunAggregator(
        run_id=settings.run_id,
        run_env="test",
        store_codes=["A100"],
    )

    async def fake_downloads(*, settings, logger):
        return {"nonpackage_all": 1}

    async def fake_reporting(*, settings, logger, aggregator, report_date):
        return report_date

    async def fake_finalize(*, settings, logger, aggregator, report_date):
        return None

    monkeypatch.setattr(dashboard_pipeline, "run_all_stores_single_session", fake_downloads)
    monkeypatch.setattr(dashboard_pipeline, "_run_reporting_tail_step", fake_reporting)
    monkeypatch.setattr(dashboard_pipeline, "_finalize_summary_and_email", fake_finalize)

    await dashboard_pipeline.run_pipeline(settings=settings, logger=logger, aggregator=aggregator)
