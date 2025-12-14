import asyncio
import io
import json

import pytest

from app.dashboard_downloader.run_downloads import (
    NavigationCertificateError,
    navigate_with_retry,
)


class FakePage:
    def __init__(self, *, exception: Exception) -> None:
        self.exception = exception
        self.goto_calls = 0

    async def goto(self, *args, **kwargs):  # pragma: no cover - signature parity
        self.goto_calls += 1
        raise self.exception


def run(coro):
    return asyncio.run(coro)


def test_navigate_with_retry_raises_on_certificate_errors(monkeypatch):
    cert_exception = Exception("net::ERR_CERT_COMMON_NAME_INVALID")
    page = FakePage(exception=cert_exception)

    async def never_sleep(*_args, **_kwargs):
        raise AssertionError("sleep should not be called for certificate errors")

    monkeypatch.setattr(asyncio, "sleep", never_sleep)

    with pytest.raises(NavigationCertificateError):
        run(
            navigate_with_retry(
                page,
                url="https://example.com",
                timeout_ms=1000,
                logger=None,
                store_code="123",
            )
        )

    assert page.goto_calls == 1


def test_navigate_with_retry_logs_fatal_status_on_certificate_errors(monkeypatch):
    cert_exception = Exception("net::ERR_CERT_DATE_INVALID")
    page = FakePage(exception=cert_exception)
    log_stream = io.StringIO()

    async def never_sleep(*_args, **_kwargs):
        raise AssertionError("sleep should not be called for certificate errors")

    monkeypatch.setattr(asyncio, "sleep", never_sleep)

    from app.dashboard_downloader.json_logger import JsonLogger

    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    with pytest.raises(NavigationCertificateError):
        run(
            navigate_with_retry(
                page,
                url="https://example.com",
                timeout_ms=1000,
                logger=logger,
                store_code="123",
            )
        )

    logged = [json.loads(line) for line in log_stream.getvalue().splitlines() if line.strip()]
    assert any(entry.get("status") == "fatal" for entry in logged)

