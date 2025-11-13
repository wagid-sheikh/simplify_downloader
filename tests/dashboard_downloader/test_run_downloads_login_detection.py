import asyncio

from dashboard_downloader import page_selectors
from dashboard_downloader.run_downloads import _is_login_page


class FakeLocator:
    def __init__(self, count: int):
        self._count = count

    async def count(self) -> int:
        return self._count


class FakePage:
    def __init__(self, *, url: str = "", locator_count: int = 0, html: str = "") -> None:
        self.url = url
        self._locator_count = locator_count
        self._html = html

    def locator(self, selector: str) -> FakeLocator:
        assert selector == page_selectors.LOGIN_USERNAME
        return FakeLocator(self._locator_count)

    async def content(self) -> str:
        return self._html


def run(coro):
    return asyncio.run(coro)


def test_is_login_page_detects_login_in_url():
    page = FakePage(url="https://example.com/login")
    assert run(_is_login_page(page)) is True


def test_is_login_page_detects_locator_presence():
    page = FakePage(url="https://example.com/dashboard", locator_count=1)
    assert run(_is_login_page(page)) is True


def test_is_login_page_detects_login_html_fallback():
    html = """
        <html>
            <body>
                <form>
                    <input type='text' name='username' />
                    <input type='password' name='password' />
                </form>
            </body>
        </html>
    """
    page = FakePage(url="https://example.com/dashboard", locator_count=0, html=html)
    assert run(_is_login_page(page)) is True


def test_is_login_page_returns_false_for_dashboard():
    html = """
        <html>
            <body>
                <h1>Dashboard</h1>
            </body>
        </html>
    """
    page = FakePage(url="https://example.com/dashboard", locator_count=0, html=html)
    assert run(_is_login_page(page)) is False
