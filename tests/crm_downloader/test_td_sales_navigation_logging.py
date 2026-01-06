import io
import json

from app.crm_downloader.td_orders_sync.main import _log_sales_navigation_attempt_event
from app.dashboard_downloader.json_logger import JsonLogger


def test_sales_navigation_attempt_logging_includes_page_title_once() -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    attempt = {
        "navigation_method": "orders_left_nav",
        "navigation_path": "orders_left_nav",
        "attempt": "initial",
        "retry_status": "initial",
        "success": False,
        "final_url": "https://example.com/store/orders",
        "target_url": "https://example.com/store/App/Reports/NEWSalesAndDeliveryReport",
        "nav_selector": "#achrOrderReport",
        "sales_nav_selector": "/store/App/Reports/NEWSalesAndDeliveryReport",
        "reason": "login_redirect",
        "url_transitions": [],
        "nav_samples": [],
        "page_title": "Sales report",
    }

    _log_sales_navigation_attempt_event(logger=logger, store_code="A100", attempt=attempt)

    log = json.loads(output.getvalue())
    assert log["page_title"] == "Sales report"
    assert log["navigation_method"] == "orders_left_nav"
    assert log["store_code"] == "A100"
