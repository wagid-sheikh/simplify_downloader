import asyncio

from app.dashboard_downloader import page_selectors
from app.dashboard_downloader.config import LOGIN_URL
from app.dashboard_downloader.run_downloads import (
    _is_login_page,
    _looks_like_login_html_bytes,
)


class FakeLocator:
    def __init__(self, count: int):
        self._count = count

    async def count(self) -> int:
        return self._count


class FakePage:
    def __init__(
        self,
        *,
        url: str = "",
        locator_counts: dict[str, int] | None = None,
        html: str = "",
    ) -> None:
        self.url = url
        self._locator_counts = locator_counts or {}
        self._html = html

    def locator(self, selector: str) -> FakeLocator:
        return FakeLocator(self._locator_counts.get(selector, 0))

    async def content(self) -> str:
        return self._html


def run(coro):
    return asyncio.run(coro)


def test_is_login_page_detects_login_in_url():
    page = FakePage(url=LOGIN_URL)
    assert run(_is_login_page(page)) is True


def test_is_login_page_detects_locator_presence():
    page = FakePage(
        url="https://example.com/dashboard",
        locator_counts={
            page_selectors.LOGIN_USERNAME: 1,
            page_selectors.LOGIN_PASSWORD: 1,
        },
    )
    assert run(_is_login_page(page)) is True


def test_is_login_page_detects_login_html_fallback():
    html = """
        <html>
            <body>
                <form>
                    <input type='text' name='user_name' />
                    <input type='password' name='password' />
                </form>
            </body>
        </html>
    """
    page = FakePage(url="https://example.com/dashboard", html=html)
    assert run(_is_login_page(page)) is True


def test_is_login_page_detects_login_html_from_selectors():
    html = f"""
        <html>
            <body>
                <form>
                    <input type='text' name='user_name' />
                    <{page_selectors.LOGIN_PASSWORD.split('[')[0]} type='password' name='password' />
                </form>
            </body>
        </html>
    """
    page = FakePage(url="https://example.com/dashboard", html=html)
    assert run(_is_login_page(page)) is True


def test_is_login_page_returns_false_for_dashboard():
    html = """
        <html>
            <body>
                <h1>Dashboard</h1>
            </body>
        </html>
    """
    page = FakePage(url="https://example.com/dashboard", html=html)
    assert run(_is_login_page(page)) is False


def test_looks_like_login_html_bytes_detects_login_markup():
    payload = b"""
        <html>
            <body>
                <input type='text' name='user_name'>
                <input type='password' name='password'>
                <button type='submit'>Log In</button>
            </body>
        </html>
    """

    assert _looks_like_login_html_bytes(payload) is True


def test_looks_like_login_html_bytes_rejects_csv():
    payload = b"header1,header2\nvalue1,value2\n"
    assert _looks_like_login_html_bytes(payload) is False
