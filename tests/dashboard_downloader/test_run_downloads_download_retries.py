import errno
import io
import json

import pytest

from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.run_summary import RunAggregator
from app.dashboard_downloader.run_downloads import _download_one_spec
from app.dashboard_downloader.settings import PipelineSettings


class _FakeResponse:
    def __init__(self, *, status: int = 200, body: bytes = b"a,b\n1,2\n") -> None:
        self.status = status
        self._body = body

    async def body(self) -> bytes:
        return self._body


class _FakeRequest:
    def __init__(self, outcomes):
        self._outcomes = list(outcomes)

    async def get(self, _url: str):
        if not self._outcomes:
            return _FakeResponse()
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _FakeContext:
    def __init__(self, outcomes):
        self.request = _FakeRequest(outcomes)


class _FakePage:
    def __init__(self, outcomes):
        self.context = _FakeContext(outcomes)


@pytest.mark.parametrize(
    "download_error",
    [
        Exception("ETIMEDOUT"),
        Exception("ECONNRESET"),
        Exception("ECONNREFUSED"),
        Exception("socket hang up"),
        Exception("read ETIMEDOUT"),
        Exception("network changed"),
        OSError(errno.ECONNREFUSED, "structured socket failure"),
    ],
    ids=[
        "etimedout",
        "econnreset",
        "econnrefused",
        "socket-hang-up",
        "read-etimedout",
        "network-changed",
        "structured-errno",
    ],
)
@pytest.mark.asyncio
async def test_download_one_spec_retries_transient_then_succeeds(
    monkeypatch, tmp_path, download_error
):
    monkeypatch.setattr("app.dashboard_downloader.run_downloads.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "app.dashboard_downloader.run_downloads.asyncio.sleep",
        lambda *_args, **_kwargs: _noop(),
    )

    stream = io.StringIO()
    logger = JsonLogger(run_id="run-retry-success", stream=stream, log_file_path=None)
    settings = PipelineSettings(run_id="run-retry-success", stores={}, raw_store_env="")

    page = _FakePage([download_error, _FakeResponse()])
    store_cfg = {"store_code": "A100"}
    spec = {
        "key": "nonpackage_all",
        "url_template": "https://example.com/file?store_code={sc}",
        "out_name_template": "{sc}-nonpackage.csv",
    }

    saved_path, _ = await _download_one_spec(
        page,
        store_cfg,
        spec,
        logger=logger,
        nav_timeout_ms=100,
        settings=settings,
    )

    assert saved_path is not None
    assert saved_path.exists()

    entries = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
    retry_logs = [
        entry
        for entry in entries
        if entry.get("message") == "transient request failure for nonpackage_all — retrying"
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0]["extras"]["attempt"] == 1
    assert retry_logs[0]["extras"]["max_attempts"] == 3


@pytest.mark.asyncio
async def test_download_one_spec_retries_playwright_read_etimedout_without_summary_error(
    monkeypatch, tmp_path
):
    monkeypatch.setattr("app.dashboard_downloader.run_downloads.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "app.dashboard_downloader.run_downloads.asyncio.sleep",
        lambda *_args, **_kwargs: _noop(),
    )

    stream = io.StringIO()
    logger = JsonLogger(run_id="run-playwright-etimedout", stream=stream, log_file_path=None)
    aggregator = RunAggregator(
        run_id="run-playwright-etimedout", run_env="test", store_codes=["A100"]
    )
    logger.aggregator = aggregator
    settings = PipelineSettings(run_id="run-playwright-etimedout", stores={}, raw_store_env="")

    page = _FakePage(
        [Exception("APIRequestContext.get: read ETIMEDOUT"), _FakeResponse()]
    )
    store_cfg = {"store_code": "A100"}
    spec = {
        "key": "nonpackage_all",
        "url_template": "https://example.com/file?store_code={sc}",
        "out_name_template": "{sc}-nonpackage.csv",
    }

    saved_path, _ = await _download_one_spec(
        page,
        store_cfg,
        spec,
        logger=logger,
        nav_timeout_ms=100,
        settings=settings,
    )

    assert saved_path is not None
    assert saved_path.exists()

    entries = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
    retry_logs = [
        entry
        for entry in entries
        if entry.get("message") == "transient request failure for nonpackage_all — retrying"
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0]["status"] == "warning"
    assert not [entry for entry in entries if entry["status"] == "error"]
    assert aggregator.overall_status() == "warning"


@pytest.mark.asyncio
async def test_download_one_spec_emits_single_final_error_with_retry_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr("app.dashboard_downloader.run_downloads.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "app.dashboard_downloader.run_downloads.asyncio.sleep",
        lambda *_args, **_kwargs: _noop(),
    )

    stream = io.StringIO()
    logger = JsonLogger(run_id="run-retry-fail", stream=stream, log_file_path=None)
    settings = PipelineSettings(run_id="run-retry-fail", stores={}, raw_store_env="")

    page = _FakePage([
        Exception("socket hang up"),
        Exception("connection reset by peer"),
        Exception("timed out"),
    ])
    store_cfg = {"store_code": "A100"}
    spec = {
        "key": "repeat_customers",
        "url_template": "https://example.com/file?store_code={sc}",
        "out_name_template": "{sc}-repeat.csv",
    }

    saved_path, _ = await _download_one_spec(
        page,
        store_cfg,
        spec,
        logger=logger,
        nav_timeout_ms=100,
        settings=settings,
    )

    assert saved_path is None

    entries = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
    final_errors = [
        entry
        for entry in entries
        if entry.get("message") == "request failed for repeat_customers"
    ]
    assert len(final_errors) == 1
    assert final_errors[0]["status"] == "error"
    retry_logs = [
        entry
        for entry in entries
        if entry.get("message") == "transient request failure for repeat_customers — retrying"
    ]
    assert len(retry_logs) == 2
    assert {entry["status"] for entry in retry_logs} == {"warning"}
    assert len([entry for entry in entries if entry["status"] == "error"]) == 1
    retry_meta = final_errors[0]["extras"]["retry"]
    assert retry_meta["max_attempts"] == 3
    assert retry_meta["retry_count"] == 2
    assert len(retry_meta["retry_errors"]) == 2


@pytest.mark.asyncio
async def test_download_one_spec_redacts_playwright_request_headers_from_jsonl(monkeypatch, tmp_path):
    monkeypatch.setattr("app.dashboard_downloader.run_downloads.DATA_DIR", tmp_path)

    stream = io.StringIO()
    logger = JsonLogger(run_id="run-redaction", stream=stream, log_file_path=None)
    settings = PipelineSettings(run_id="run-redaction", stores={}, raw_store_env="")
    playwright_error = Exception(
        "APIRequestContext.get: request failed\n"
        "cookie: dashboard-session-secret\n"
        "authorization: Bearer dashboard-auth-secret"
    )
    page = _FakePage([playwright_error])
    store_cfg = {"store_code": "A100"}
    spec = {
        "key": "repeat_customers",
        "url_template": "https://example.com/file?store_code={sc}",
        "out_name_template": "{sc}-repeat.csv",
    }

    saved_path, _ = await _download_one_spec(
        page,
        store_cfg,
        spec,
        logger=logger,
        nav_timeout_ms=100,
        settings=settings,
    )

    assert saved_path is None
    output = stream.getvalue()
    assert "dashboard-session-secret" not in output
    assert "dashboard-auth-secret" not in output
    assert output.count("<redacted>") == 2
    event = json.loads(output)
    assert event["message"] == "request failed for repeat_customers"
    assert event["extras"]["error"].count("<redacted>") == 2


async def _noop():
    return None
