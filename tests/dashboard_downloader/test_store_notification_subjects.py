from __future__ import annotations

from datetime import date
from pathlib import Path

from app.dashboard_downloader.notifications import DocumentRecord, _build_store_plans


def test_store_plans_render_subject_with_store_code(tmp_path: Path) -> None:
    for store_code in ("TD101", "UC202"):
        (tmp_path / f"{store_code}.pdf").write_bytes(b"pdf")

    docs = [
        DocumentRecord(
            doc_type="store_daily_pdf",
            store_code="TD101",
            path=tmp_path / "TD101.pdf",
        ),
        DocumentRecord(
            doc_type="store_daily_pdf",
            store_code="UC202",
            path=tmp_path / "UC202.pdf",
        ),
    ]

    profile = {"code": "store_daily_reports", "scope": "store", "attach_mode": "per_store_pdf"}
    template = {
        "subject_template": "TD Orders Sync – {{ store_code }}",
        "body_template": "Run {{ run_id }} for {{ store_code }}",
    }
    recipients = [
        {"store_code": "TD101", "email_address": "td@example.com", "send_as": "to"},
        {"store_code": "UC202", "email_address": "uc@example.com", "send_as": "to"},
    ]

    plans = _build_store_plans(
        pipeline_code="dashboard_daily",
        profile=profile,
        template=template,
        recipients=recipients,
        docs=docs,
        context={"run_id": "dry-run-1", "report_date": date(2024, 1, 1)},
        store_names={"TD101": "TD Store", "UC202": "UC Store"},
    )

    subjects = [plan.subject for plan in plans]

    assert subjects == [
        "TD Orders Sync – TD101",
        "TD Orders Sync – UC202",
    ]
    assert all(plan.store_code in plan.subject for plan in plans if plan.store_code)



def test_store_plans_for_td_uc_profile_without_dashboard_pdf_docs() -> None:
    profile = {"code": "store_reports", "scope": "store", "attach_mode": "per_store_pdf"}
    template = {
        "subject_template": "Store {{ store_code }} run {{ run_id }}",
        "body_template": "Body for {{ store_name }}",
    }
    recipients = [
        {"store_code": "TD101", "email_address": "td@example.com", "send_as": "to"},
        {"store_code": "UC202", "email_address": "uc@example.com", "send_as": "to"},
    ]

    plans = _build_store_plans(
        pipeline_code="td_orders_sync",
        profile=profile,
        template=template,
        recipients=recipients,
        docs=[],
        context={
            "run_id": "run-1",
            "report_date": date(2024, 1, 1),
            "stores": [{"store_code": "TD101"}, {"store_code": "UC202"}],
        },
        store_names={"TD101": "TD One", "UC202": "UC Two"},
    )

    assert [plan.store_code for plan in plans] == ["TD101", "UC202"]
    assert [plan.subject for plan in plans] == ["Store TD101 run run-1", "Store UC202 run run-1"]
    assert all(plan.attachments == [] for plan in plans)


def test_store_plans_include_recipient_only_store_codes_without_docs() -> None:
    profile = {"code": "store_reports", "scope": "store", "attach_mode": "per_store_pdf"}
    template = {
        "subject_template": "Store {{ store_code }}",
        "body_template": "Body {{ store_code }}",
    }
    recipients = [{"store_code": "TD303", "email_address": "td303@example.com", "send_as": "to"}]

    plans = _build_store_plans(
        pipeline_code="uc_orders_sync",
        profile=profile,
        template=template,
        recipients=recipients,
        docs=[],
        context={"run_id": "run-2", "report_date": date(2024, 1, 1), "stores": []},
        store_names={"TD303": "TD Three"},
    )

    assert len(plans) == 1
    assert plans[0].store_code == "TD303"
    assert plans[0].to == ["td303@example.com"]
