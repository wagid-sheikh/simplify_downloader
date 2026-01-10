import io
import json

from app.config import config
from app.crm_downloader.td_orders_sync import main as td_orders_main
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


def test_sales_navigation_attempt_logging_skips_dom_fields(monkeypatch: object) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    monkeypatch.setattr(config, "pipeline_skip_dom_logging", True)
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
        "nav_samples": [{"selector": "#target"}],
        "links": [{"href": "https://example.com"}],
        "reports_links": [{"href": "https://example.com/reports"}],
        "row_samples": [{"row": "sample"}],
        "observed_controls": ["orders"],
        "observed_spinners": ["loading"],
        "matched_range_examples": ["range 1"],
        "page_title": "Sales report",
    }

    _log_sales_navigation_attempt_event(logger=logger, store_code="A100", attempt=attempt)

    log = json.loads(output.getvalue())
    for key in td_orders_main.DOM_LOGGING_FIELDS:
        assert key not in log


def test_scrub_dom_logging_fields_removes_dom_keys() -> None:
    payload = {
        "navigation_method": "orders_left_nav",
        "nav_samples": [{"selector": "#target"}],
        "links": [{"href": "https://example.com"}],
        "reports_links": [{"href": "https://example.com/reports"}],
        "row_samples": [{"row": "sample"}],
        "observed_controls": ["orders"],
        "observed_spinners": ["loading"],
        "matched_range_examples": ["range 1"],
    }

    scrubbed = td_orders_main._scrub_dom_logging_fields(payload)

    assert scrubbed == {"navigation_method": "orders_left_nav"}
    assert payload["nav_samples"] == [{"selector": "#target"}]
