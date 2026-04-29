from app.reports.shared.line_items_summary import summarize_line_items


def test_summarize_line_items_skips_blank_and_null() -> None:
    assert summarize_line_items([
        {"service_name": None, "garment_name": None},
        {"service_name": "  ", "garment_name": ""},
    ]) == ""


def test_summarize_line_items_mixed_service_and_garment_presence() -> None:
    result = summarize_line_items([
        {"service_name": " Wash ", "garment_name": " Shirt "},
        {"service_name": "Iron", "garment_name": ""},
        {"service_name": None, "garment_name": "Trouser"},
    ])
    assert result == "Iron × 1 | Trouser × 1 | Wash Shirt × 1"


def test_summarize_line_items_numbered_ranges_contiguous_and_non_contiguous() -> None:
    result = summarize_line_items([
        {"service_name": "WF", "garment_name": "Garment 1"},
        {"service_name": "WF", "garment_name": "Garment 2"},
        {"service_name": "WF", "garment_name": "Garment 3"},
        {"service_name": "WF", "garment_name": "Garment 7"},
        {"service_name": "WF", "garment_name": "Garment 7"},
        {"service_name": "WF", "garment_name": "Garment 9"},
    ])
    assert result == "WF Garment 1–3, 7, 9"


def test_summarize_line_items_non_numbered_counts() -> None:
    result = summarize_line_items([
        {"service_name": "Dryclean", "garment_name": "Shirt"},
        {"service_name": "Dryclean", "garment_name": "Shirt"},
    ])
    assert result == "Dryclean Shirt × 2"


def test_summarize_line_items_deterministic_ordering() -> None:
    result = summarize_line_items([
        {"service_name": "b", "garment_name": "item"},
        {"service_name": "WF", "garment_name": "Garment 10"},
        {"service_name": "a", "garment_name": "item"},
        {"service_name": "WF", "garment_name": "Garment 2"},
    ])
    assert result == "WF Garment 2, 10 | a item × 1 | b item × 1"
