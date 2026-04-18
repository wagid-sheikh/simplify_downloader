from pathlib import Path
from types import SimpleNamespace

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main


class _FakeLocator:
    def __init__(self, count_value: int) -> None:
        self._count_value = count_value

    async def count(self) -> int:
        return self._count_value


class _FakeSignaturePage:
    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = counts

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self._counts.get(selector, 0))


class _FakeDomPage:
    def __init__(self, html: str) -> None:
        self._html = html
        self.screenshot_called = False

    async def content(self) -> str:
        return self._html

    async def screenshot(self, *, path: str, **_: object) -> None:
        Path(path).write_bytes(b"fake")
        self.screenshot_called = True


@pytest.mark.asyncio
async def test_get_login_page_version_signature_includes_selector_presence() -> None:
    page = _FakeSignaturePage(
        {
            "#email": 1,
            "#password": 1,
            "button.btn-primary[type='submit']": 0,
            "button#login": 1,
            "button#submit": 0,
        }
    )

    signature = await uc_main._get_login_page_version_signature(page=page)

    assert signature == (
        "email_id=1,password_id=1,submit_btn_primary=0,"
        "submit_id_login=1,submit_id_submit=0"
    )


@pytest.mark.asyncio
async def test_capture_login_dom_mismatch_artifacts_persists_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    page = _FakeDomPage("<html><body>login</body></html>")
    store = uc_main.UcStore(
        store_code="A100", store_name="Store A", cost_center=None, sync_config={}
    )
    monkeypatch.setattr(
        uc_main, "config", SimpleNamespace(pipeline_skip_dom_logging=False)
    )
    monkeypatch.setattr(uc_main, "default_download_dir", lambda: tmp_path)

    artifacts = await uc_main._capture_login_dom_mismatch_artifacts(
        page=page, store=store
    )

    assert "login_dom_snippet_path" in artifacts
    assert "login_dom_screenshot_path" in artifacts
    assert Path(artifacts["login_dom_snippet_path"]).exists()
    assert Path(artifacts["login_dom_screenshot_path"]).exists()
    assert page.screenshot_called is True
