from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.crm_downloader.td_leads_sync.ingest import build_lead_uid
from app.crm_downloader.td_leads_sync.main import _field_from_headers


def test_build_lead_uid_is_stable_for_same_business_identity() -> None:
    base = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    uid_a = build_lead_uid(base)
    uid_b = build_lead_uid(dict(base))
    assert uid_a == uid_b


def test_build_lead_uid_changes_when_status_changes() -> None:
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    pending_uid = build_lead_uid(row)
    cancelled_uid = build_lead_uid({**row, "status_bucket": "cancelled"})
    assert pending_uid != cancelled_uid


@pytest.mark.parametrize(
    ("field_name", "expected"),
    [
        ("pickup_no", "A668-3025"),
        ("customer_name", "moni"),
        ("mobile", "9599242207"),
        ("pickup_date", "22 Apr 2026"),
        ("pickup_time", "11:00 AM - 1:00 PM"),
        ("special_instruction", "Leave at door"),
        ("status_text", "CANCELLED"),
        ("reason", "enquiry"),
        ("source", "Facebook"),
        ("user", "Super Admin"),
    ],
)
def test_field_from_headers_uses_header_name_mapping(field_name: str, expected: str) -> None:
    headers = [
        "S No.",
        "Pickup No.",
        "Customer Name",
        "Address",
        "Mobile",
        "Pickup Date",
        "Pickup Time",
        "Special Instruction",
        "Status",
        "Reason",
        "Source",
        "User",
    ]
    values = [
        "1",
        "A668-3025",
        "moni",
        "Address line",
        "9599242207",
        "22 Apr 2026",
        "11:00 AM - 1:00 PM",
        "Leave at door",
        "CANCELLED",
        "enquiry",
        "Facebook",
        "Super Admin",
    ]

    resolved = _field_from_headers(headers=headers, values=values, field_name=field_name)
    assert resolved == expected


def test_field_from_headers_returns_none_when_alias_not_present() -> None:
    assert _field_from_headers(headers=["Foo"], values=["Bar"], field_name="pickup_no") is None


def test_scraped_at_value_can_pass_through() -> None:
    now_utc = datetime.now(timezone.utc)
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "1",
        "pickup_no": "A668-1",
        "mobile": "9999999999",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
        "scraped_at": now_utc,
    }
    assert build_lead_uid(row)
