from __future__ import annotations

import pytest

from app.reports.payment_evidence_review import (
    PaymentEvidenceReviewFilters,
    build_payment_evidence_review_query,
)


def test_payment_evidence_review_query_includes_requested_filters() -> None:
    query, params = build_payment_evidence_review_query(
        PaymentEvidenceReviewFilters(
            source_type="google_sheet",
            cost_center="CC1",
            start_date="2026-05-01",
            end_date="2026-05-15",
            grouped=True,
            limit=100,
        )
    )

    sql = str(query).lower()

    assert "from vw_payment_evidence_reconciliation" in sql
    assert "source_type = :source_type" in sql
    assert "cost_center = :cost_center" in sql
    assert "payment_date >= :start_date" in sql
    assert "payment_date <= :end_date" in sql
    assert "is_grouped = :grouped" in sql
    assert "limit :limit" in sql
    assert params == {
        "source_type": "google_sheet",
        "cost_center": "CC1",
        "start_date": "2026-05-01",
        "end_date": "2026-05-15",
        "grouped": True,
        "limit": 100,
    }


def test_payment_evidence_review_query_rejects_nonpositive_limit() -> None:
    with pytest.raises(ValueError, match="limit must be greater than zero"):
        build_payment_evidence_review_query(PaymentEvidenceReviewFilters(limit=0))
