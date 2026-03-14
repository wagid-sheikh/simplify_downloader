from __future__ import annotations

from datetime import date

import pytest

from app.crm_downloader.td_orders_sync import main as td_main


def test_td_parser_defaults_source_mode_to_api_only() -> None:
    parser = td_main._build_parser()
    args = parser.parse_args([])

    assert args.source_mode == "api_only"


def test_td_parser_still_accepts_explicit_ui_mode() -> None:
    parser = td_main._build_parser()
    args = parser.parse_args(["--source-mode", "ui"])

    assert args.source_mode == "ui"


def test_td_parser_invalid_source_mode_validation_unchanged() -> None:
    parser = td_main._build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--source-mode", "invalid"])


@pytest.mark.asyncio
async def test_td_main_defaults_to_api_only_when_source_mode_none(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    async def _fake_load_td_order_stores(*, logger, store_codes=None):
        return []

    def _fake_log_event(*, logger, phase, message, **kwargs):
        if message == "Resolved TD ingest source for mode":
            captured["source_mode"] = kwargs["source_mode"]

    monkeypatch.setattr(td_main, "_load_td_order_stores", _fake_load_td_order_stores)
    monkeypatch.setattr(td_main, "log_event", _fake_log_event)

    await td_main.main(to_date=date(2026, 1, 1), source_mode=None)

    assert captured["source_mode"] == "api_only"


@pytest.mark.asyncio
async def test_td_main_still_accepts_explicit_ui_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    async def _fake_load_td_order_stores(*, logger, store_codes=None):
        return []

    def _fake_log_event(*, logger, phase, message, **kwargs):
        if message == "Resolved TD ingest source for mode":
            captured["source_mode"] = kwargs["source_mode"]

    monkeypatch.setattr(td_main, "_load_td_order_stores", _fake_load_td_order_stores)
    monkeypatch.setattr(td_main, "log_event", _fake_log_event)

    await td_main.main(to_date=date(2026, 1, 1), source_mode="ui")

    assert captured["source_mode"] == "ui"


@pytest.mark.asyncio
async def test_td_main_invalid_source_mode_validation_unchanged() -> None:
    with pytest.raises(ValueError, match="source_mode must be one of"):
        await td_main.main(to_date=date(2026, 1, 1), source_mode="invalid")
