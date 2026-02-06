import io
import json

from types import SimpleNamespace

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


@pytest.mark.asyncio
async def test_selector_cue_logging_skipped_when_dom_logging_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    monkeypatch.setattr(uc_main, "config", SimpleNamespace(pipeline_skip_dom_logging=True))

    await uc_main._log_selector_cues(logger=logger, store_code="A100", container=object(), page=object())

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
