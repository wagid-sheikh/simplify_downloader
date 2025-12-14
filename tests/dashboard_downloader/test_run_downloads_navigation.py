import asyncio
import io
import json

import pytest

from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.run_downloads import (
    NavigationCertificateError,
    navigate_with_retry,
)


class FakeContext:
    def __init__(self, *, ignore_https_errors: bool = False, succeed_when_insecure: bool = True):
        self.ignore_https_errors = ignore_https_errors
        self.succeed_when_insecure = succeed_when_insecure
        self.new_context_calls = 0
        self.browser = self

    async def storage_state(self):  # pragma: no cover - parity
        return {}

    async def new_context(self, **kwargs):
        self.new_context_calls += 1
        return FakeContext(
            ignore_https_errors=kwargs.get("ignore_https_errors", False),
            succeed_when_insecure=self.succeed_when_insecure,
        )

    async def new_page(self):
        return FakePage(
            context=self, succeed_when_insecure=self.succeed_when_insecure
        )


class FakePage:
    def __init__(
        self,
        *,
        context: FakeContext,
        succeed_when_insecure: bool = True,
        cert_error_on_secure: bool = True,
    ) -> None:
        self.context = context
        self.succeed_when_insecure = succeed_when_insecure
        self.cert_error_on_secure = cert_error_on_secure
        self.goto_calls = 0
        self.closed = False

    async def goto(self, *args, **kwargs):  # pragma: no cover - signature parity
        self.goto_calls += 1
        if self.cert_error_on_secure and not self.context.ignore_https_errors:
            raise Exception("net::ERR_CERT_COMMON_NAME_INVALID")
        if not self.succeed_when_insecure:
            raise Exception("net::ERR_CERT_COMMON_NAME_INVALID")
        return {"ok": True}

    async def close(self):  # pragma: no cover - cleanup parity
        self.closed = True


def run(coro):
    return asyncio.run(coro)


async def _no_sleep(*_args, **_kwargs):  # pragma: no cover - async helper
    return None


def test_navigate_with_retry_switches_to_insecure_mode(monkeypatch):
    log_stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    context = FakeContext()
    page = FakePage(context=context)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    new_page, response = run(
        navigate_with_retry(
            page,
            url="https://example.com",
            timeout_ms=1000,
            logger=logger,
            store_code="123",
        )
    )

    assert response == {"ok": True}
    assert new_page is not page
    assert page.closed is True
    assert page.goto_calls == 1
    assert new_page.goto_calls == 1
    assert new_page.context.ignore_https_errors is True
    assert context.new_context_calls == 1

    logged = [json.loads(line) for line in log_stream.getvalue().splitlines() if line.strip()]
    warn_entries = [
        entry for entry in logged if entry.get("status") == "warn"
    ]
    assert any(entry.get("extras", {}).get("ignore_https_errors") for entry in warn_entries)
    assert any(entry.get("extras", {}).get("insecure_retry") for entry in warn_entries)

    recreate_entries = [
        entry
        for entry in logged
        if entry.get("status") == "info"
        and entry.get("message") == "recreated page with HTTPS checks disabled"
    ]
    assert recreate_entries
    for entry in recreate_entries:
        extras = entry.get("extras", {})
        assert extras.get("attempt") == 1
        assert extras.get("insecure_retry") is True
        assert extras.get("page_recreated") is True
        assert extras.get("ignore_https_errors") is True

    wait_entries = [
        entry
        for entry in logged
        if entry.get("message") == "waiting before retry"
    ]
    assert wait_entries
    assert any(entry.get("extras", {}).get("attempt") == 1 for entry in wait_entries)
    assert any(entry.get("extras", {}).get("wait_time_s") == 0 for entry in wait_entries)


def test_navigate_with_retry_logs_fatal_after_insecure_failure(monkeypatch):
    log_stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    context = FakeContext(succeed_when_insecure=False)
    page = FakePage(context=context, succeed_when_insecure=False)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

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
    statuses = [entry.get("status") for entry in logged]
    assert "warn" in statuses
    assert "fatal" in statuses


def test_navigate_with_retry_prefers_secure_context(monkeypatch):
    log_stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    context = FakeContext()
    page = FakePage(context=context, cert_error_on_secure=False)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    new_page, response = run(
        navigate_with_retry(
            page,
            url="https://example.com",
            timeout_ms=1000,
            logger=logger,
            store_code="123",
        )
    )

    assert response == {"ok": True}
    assert new_page is page
    assert context.new_context_calls == 0

    logged = [json.loads(line) for line in log_stream.getvalue().splitlines() if line.strip()]
    assert all(entry.get("status") != "warn" for entry in logged)

