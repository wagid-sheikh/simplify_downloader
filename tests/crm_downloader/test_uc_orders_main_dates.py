from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main


class _Logger:
    def close(self) -> None:
        return None


class _Browser:
    async def close(self) -> None:
        return None


class _AsyncPlaywrightContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.mark.asyncio
async def test_main_honors_explicit_from_to_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_calls: list[dict[str, date]] = []

    async def _run_store_discovery(**kwargs):
        captured_calls.append(
            {"from_date": kwargs["from_date"], "to_date": kwargs["to_date"]}
        )

    monkeypatch.setattr(
        uc_main,
        "config",
        SimpleNamespace(
            run_env="test", database_url="postgres://db", pipeline_skip_dom_logging=True
        ),
    )
    monkeypatch.setattr(uc_main, "new_run_id", lambda: "run-1")
    monkeypatch.setattr(
        uc_main, "aware_now", lambda _tz: datetime(2024, 2, 15, tzinfo=timezone.utc)
    )
    monkeypatch.setattr(uc_main, "get_logger", lambda **_: _Logger())
    monkeypatch.setattr(uc_main, "log_event", lambda **_: None)
    monkeypatch.setattr(
        uc_main,
        "_resolve_uc_archive_extraction_mode",
        lambda: uc_main.UC_ARCHIVE_EXTRACTION_MODE_API,
    )
    monkeypatch.setattr(uc_main, "_resolve_uc_max_workers", lambda: 1)
    monkeypatch.setattr(uc_main, "async_playwright", lambda: _AsyncPlaywrightContext())
    monkeypatch.setattr(uc_main, "launch_browser", lambda **_: _noop_async(_Browser()))
    monkeypatch.setattr(uc_main, "_run_store_discovery", _run_store_discovery)
    monkeypatch.setattr(
        uc_main, "_fetch_dashboard_nav_timeout_ms", lambda *_: _noop_async(90_000)
    )
    monkeypatch.setattr(uc_main, "_start_run_summary", lambda **_: _noop_async(True))
    monkeypatch.setattr(uc_main, "_persist_summary", lambda **_: _noop_async(True))
    monkeypatch.setattr(
        uc_main,
        "send_notifications_for_run",
        lambda *_: _noop_async({"emails_planned": 0, "emails_sent": 0, "errors": []}),
    )
    monkeypatch.setattr(
        uc_main, "resolve_orders_sync_start_date", lambda **kwargs: kwargs["from_date"]
    )
    monkeypatch.setattr(uc_main, "resolve_window_settings", lambda **_: (30, 30, 0))
    monkeypatch.setattr(
        uc_main,
        "_load_uc_order_stores",
        lambda **_: _noop_async(
            [
                uc_main.UcStore(
                    store_code="A100", store_name=None, cost_center=None, sync_config={}
                )
            ]
        ),
    )

    await uc_main.main(from_date=date(2024, 1, 10), to_date=date(2024, 1, 31))

    assert captured_calls == [
        {"from_date": date(2024, 1, 10), "to_date": date(2024, 1, 31)}
    ]


@pytest.mark.asyncio
async def test_main_uses_dynamic_window_when_from_date_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_resolver_from_dates: list[date | None] = []
    captured_calls: list[dict[str, date]] = []

    def _resolve_start(**kwargs):
        captured_resolver_from_dates.append(kwargs["from_date"])
        return date(2024, 1, 5)

    async def _run_store_discovery(**kwargs):
        captured_calls.append(
            {"from_date": kwargs["from_date"], "to_date": kwargs["to_date"]}
        )

    monkeypatch.setattr(
        uc_main,
        "config",
        SimpleNamespace(
            run_env="test", database_url="postgres://db", pipeline_skip_dom_logging=True
        ),
    )
    monkeypatch.setattr(uc_main, "new_run_id", lambda: "run-2")
    monkeypatch.setattr(
        uc_main, "aware_now", lambda _tz: datetime(2024, 2, 15, tzinfo=timezone.utc)
    )
    monkeypatch.setattr(uc_main, "get_logger", lambda **_: _Logger())
    monkeypatch.setattr(uc_main, "log_event", lambda **_: None)
    monkeypatch.setattr(
        uc_main,
        "_resolve_uc_archive_extraction_mode",
        lambda: uc_main.UC_ARCHIVE_EXTRACTION_MODE_API,
    )
    monkeypatch.setattr(uc_main, "_resolve_uc_max_workers", lambda: 1)
    monkeypatch.setattr(uc_main, "async_playwright", lambda: _AsyncPlaywrightContext())
    monkeypatch.setattr(uc_main, "launch_browser", lambda **_: _noop_async(_Browser()))
    monkeypatch.setattr(uc_main, "_run_store_discovery", _run_store_discovery)
    monkeypatch.setattr(
        uc_main, "_fetch_dashboard_nav_timeout_ms", lambda *_: _noop_async(90_000)
    )
    monkeypatch.setattr(uc_main, "_start_run_summary", lambda **_: _noop_async(True))
    monkeypatch.setattr(uc_main, "_persist_summary", lambda **_: _noop_async(True))
    monkeypatch.setattr(
        uc_main,
        "send_notifications_for_run",
        lambda *_: _noop_async({"emails_planned": 0, "emails_sent": 0, "errors": []}),
    )
    monkeypatch.setattr(uc_main, "resolve_orders_sync_start_date", _resolve_start)
    monkeypatch.setattr(uc_main, "resolve_window_settings", lambda **_: (30, 30, 2))
    monkeypatch.setattr(uc_main, "_fetch_pipeline_id", lambda **_: _noop_async(123))
    monkeypatch.setattr(
        uc_main,
        "fetch_last_success_window_end",
        lambda **_: _noop_async(date(2024, 1, 20)),
    )
    monkeypatch.setattr(
        uc_main,
        "_load_uc_order_stores",
        lambda **_: _noop_async(
            [
                uc_main.UcStore(
                    store_code="A100", store_name=None, cost_center=None, sync_config={}
                )
            ]
        ),
    )

    await uc_main.main(to_date=date(2024, 1, 31))

    assert captured_resolver_from_dates == [None]
    assert captured_calls == [
        {"from_date": date(2024, 1, 5), "to_date": date(2024, 1, 31)}
    ]


@pytest.mark.asyncio
async def test_async_entrypoint_passes_cli_dates_to_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, date | str | None] = {}

    async def _main(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(uc_main, "main", _main)

    await uc_main._async_entrypoint(
        [
            "--run-env",
            "prod",
            "--run-id",
            "run-cli",
            "--from-date",
            "2024-01-01",
            "--to-date",
            "2024-01-31",
        ]
    )

    assert captured == {
        "run_env": "prod",
        "run_id": "run-cli",
        "from_date": date(2024, 1, 1),
        "to_date": date(2024, 1, 31),
    }


async def _noop_async(value):
    return value
