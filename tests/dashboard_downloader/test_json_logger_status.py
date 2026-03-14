import io

import pytest

from app.dashboard_downloader.json_logger import JsonLogger, LOG_STATUSES


def test_logger_accepts_canonical_warning_status() -> None:
    logger = JsonLogger(run_id="test", stream=io.StringIO(), log_file_path=None)
    logger.info(phase="unit", status="warning", message="warning event")


def test_logger_rejects_unknown_status_value() -> None:
    logger = JsonLogger(run_id="test", stream=io.StringIO(), log_file_path=None)
    with pytest.raises(ValueError):
        logger.info(phase="unit", status="warn", message="legacy warning")


@pytest.mark.parametrize("status", sorted(LOG_STATUSES))
def test_logger_accepts_all_allowed_statuses(status: str) -> None:
    logger = JsonLogger(run_id="test", stream=io.StringIO(), log_file_path=None)
    logger.info(phase="unit", status=status, message=f"{status} event")
