from __future__ import annotations

from app.crm_downloader.td_orders_sync import main as td_orders_main


def test_get_nested_str_missing_path_returns_none() -> None:
    assert td_orders_main._get_nested_str({}, ("selectors", "reports_nav")) is None


def test_get_nested_str_coerces_tuple_value_to_first_non_empty_string() -> None:
    mapping = {"selectors": {"reports_nav": ("", "achrOrderReport")}}
    assert td_orders_main._get_nested_str(mapping, ("selectors", "reports_nav")) == "achrOrderReport"


def test_reports_nav_selector_accepts_legacy_tuple_config() -> None:
    store = td_orders_main.TdStore(
        store_code="A817",
        store_name=None,
        cost_center=None,
        sync_config={"reports_nav_selector": ("achrOrderReport",)},
    )

    assert store.reports_nav_selector == "#achrOrderReport"
