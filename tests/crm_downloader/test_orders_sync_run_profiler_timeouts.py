from __future__ import annotations

import asyncio
import fcntl
import io
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.crm_downloader.orders_sync_run_profiler.main as profiler
from app.crm_downloader.orders_sync_run_profiler.main import StoreProfile


def _events(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def _store(code: str) -> StoreProfile:
    return StoreProfile(
        store_code=code,
        store_name=f"Store {code}",
        cost_center=f"CC-{code}",
        sync_config={},
        start_date=None,
    )


async def _run_profiler_with_real_store_processing(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stores: list[StoreProfile],
    stream: io.StringIO,
    inserted_summaries: list[dict],
    pipeline_run_ids: list[str],
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )
    monkeypatch.setattr(
        profiler,
        "get_logger",
        lambda **kwargs: profiler.JsonLogger(
            run_id=kwargs.get("run_id"), stream=stream, log_file_path=None
        ),
    )

    async def fake_fetch_pipeline_id(**_kwargs: object) -> int:
        return 101

    async def fake_load_store_profiles(**_kwargs: object) -> list[StoreProfile]:
        return stores

    async def fake_pipeline_fn(**kwargs: object) -> None:
        pipeline_run_ids.append(str(kwargs["run_id"]))

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {"stores": {}},
                "sales": {"stores": {}},
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {"id": 1, "status": "success", "error_message": None}

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    async def fake_persist_missing_windows_log_rows(**_kwargs: object) -> None:
        return None

    async def fake_send_notifications_for_run(
        _pipeline_name: str, _run_id: str
    ) -> dict:
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(profiler, "_fetch_pipeline_id", fake_fetch_pipeline_id)
    monkeypatch.setattr(profiler, "_load_store_profiles", fake_load_store_profiles)
    monkeypatch.setitem(
        profiler.PIPELINE_BY_GROUP, "TD", ("td_orders_sync", fake_pipeline_fn)
    )
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        profiler,
        "_persist_missing_windows_log_rows",
        fake_persist_missing_windows_log_rows,
    )
    monkeypatch.setattr(
        profiler, "send_notifications_for_run", fake_send_notifications_for_run
    )

    await profiler.main(
        sync_group="TD",
        store_codes=None,
        max_workers=1,
        run_env="test",
        run_id="profiler-timeout-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )


@pytest.mark.asyncio
async def test_store_lock_timeout_marks_store_failed_and_continues(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_STORE_LOCK_TIMEOUT_SECONDS", "0.05")
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_WINDOW_STATE_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(profiler, "default_download_dir", lambda: tmp_path)

    lock_dir = tmp_path / "orders_sync_run_profiler_locks"
    lock_dir.mkdir(parents=True)
    lock_path = lock_dir / "LOCKED.lock"
    locked_handle = open(lock_path, "w", encoding="utf-8")
    fcntl.flock(locked_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)

    stream = io.StringIO()
    inserted_summaries: list[dict] = []
    pipeline_run_ids: list[str] = []

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end
    )

    try:
        await _run_profiler_with_real_store_processing(
            monkeypatch,
            stores=[_store("LOCKED"), _store("OPEN")],
            stream=stream,
            inserted_summaries=inserted_summaries,
            pipeline_run_ids=pipeline_run_ids,
        )
    finally:
        fcntl.flock(locked_handle, fcntl.LOCK_UN)
        locked_handle.close()

    stores_by_code = inserted_summaries[-1]["metrics_json"]["store_totals"]
    assert stores_by_code["LOCKED"]["overall_status"] == "failed"
    assert (
        stores_by_code["LOCKED"]["window_audit"][0]["failure_reason"]
        == "store_lock_timeout"
    )
    assert stores_by_code["OPEN"]["overall_status"] == "success"
    assert pipeline_run_ids == ["profiler-timeout-run_OPEN_001"]

    events = _events(stream)
    assert any(
        event["phase"] == "store_lock"
        and event["message"] == "Acquiring orders profiler store lock"
        and event["store_code"] == "LOCKED"
        and event["lock_path"] == str(lock_path)
        for event in events
    )
    assert any(
        event["phase"] == "store_lock"
        and event["message"] == "Timed out acquiring orders profiler store lock"
        and event["failure_reason"] == "store_lock_timeout"
        for event in events
    )
    assert any(
        event["phase"] == "store_lock"
        and event["message"] == "Acquired orders profiler store lock"
        and event["store_code"] == "OPEN"
        and "elapsed_ms" in event
        for event in events
    )


@pytest.mark.asyncio
async def test_window_state_timeout_marks_store_failed_logs_boundary_and_continues(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_STORE_LOCK_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_WINDOW_STATE_TIMEOUT_SECONDS", "0.05")
    monkeypatch.setattr(profiler, "default_download_dir", lambda: tmp_path)

    stream = io.StringIO()
    inserted_summaries: list[dict] = []
    pipeline_run_ids: list[str] = []

    async def fake_fetch_last_success_window_end(
        *, store_code: str, **_kwargs: object
    ) -> None:
        if store_code == "HANG":
            await asyncio.sleep(10)
        return None

    monkeypatch.setattr(
        profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end
    )

    await _run_profiler_with_real_store_processing(
        monkeypatch,
        stores=[_store("HANG"), _store("OK")],
        stream=stream,
        inserted_summaries=inserted_summaries,
        pipeline_run_ids=pipeline_run_ids,
    )

    stores_by_code = inserted_summaries[-1]["metrics_json"]["store_totals"]
    assert stores_by_code["HANG"]["overall_status"] == "failed"
    assert (
        stores_by_code["HANG"]["window_audit"][0]["failure_reason"]
        == "window_state_timeout"
    )
    assert stores_by_code["OK"]["overall_status"] == "success"
    assert pipeline_run_ids == ["profiler-timeout-run_OK_001"]

    events = _events(stream)
    assert any(
        event["phase"] == "window_state"
        and event["message"] == "Fetching last successful orders sync window state"
        and event["store_code"] == "HANG"
        for event in events
    )
    assert any(
        event["phase"] == "window_state"
        and event["message"]
        == "Timed out fetching last successful orders sync window state"
        and event["failure_reason"] == "window_state_timeout"
        for event in events
    )
    assert any(
        event["phase"] == "window_state"
        and event["message"] == "Fetched last successful orders sync window state"
        and event["store_code"] == "OK"
        and event["last_success_window_end"] is None
        and "elapsed_ms" in event
        for event in events
    )
    assert any(
        event["phase"] == "store"
        and event["message"] == "Computed window plan"
        and event["store_code"] == "OK"
        for event in events
    )
