from __future__ import annotations

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.payment_evidence_review import (
    PaymentEvidenceReviewFilters,
    build_payment_evidence_review_query,
    fetch_payment_evidence_review_rows,
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


@pytest.mark.asyncio
async def test_payment_evidence_review_fetch_uses_component_reconciliation_for_topups(
    tmp_path,
) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'payment_evidence_review.db'}"
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE vw_orders (cost_center TEXT, order_number TEXT, order_date TEXT, order_amount NUMERIC)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_received NUMERIC)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE payment_collections (payment_id INTEGER PRIMARY KEY, source_type TEXT, source_sheet_row INTEGER, cost_center TEXT, payment_date TEXT, payment_timestamp TEXT, order_number TEXT, amount NUMERIC, bank_row_id TEXT)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO vw_orders VALUES ('CC1', 'ORD1', '2026-05-01', 100), ('CC1', 'ORD2', '2026-05-01', 100)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO sales VALUES ('CC1', 'ORD1', 100), ('CC1', 'ORD2', 100)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO payment_collections VALUES (1, 'google_sheet', 11, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD1,ORD2', 150, NULL), (2, 'google_sheet', 12, 'CC1', '2026-05-02', '2026-05-02 10:01:00', 'ORD2', 50, NULL)"
            )
        )
    engine.dispose()

    async with session_scope(database_url) as session:
        rows = await fetch_payment_evidence_review_rows(
            session, PaymentEvidenceReviewFilters()
        )

    assert [row["payment_id"] for row in rows] == [2, 1]
    assert {row["reconciliation_result"] for row in rows} == {"grouped paid"}
    assert {row["group_key"] for row in rows} == {"ORD1|ORD2"}
    assert {row["grouped_amount"] for row in rows} == {200}
    assert {row["sales_evidence_difference"] for row in rows} == {0}
    assert {row["sales_evidence_mismatch"] for row in rows} == {False}


@pytest.mark.asyncio
async def test_payment_evidence_review_fetch_limits_related_order_and_sales_rows(
    tmp_path, monkeypatch
) -> None:
    database_url = (
        f"sqlite+aiosqlite:///{tmp_path / 'payment_evidence_review_filtered.db'}"
    )
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE vw_orders (cost_center TEXT, order_number TEXT, order_date TEXT, order_amount NUMERIC)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_received NUMERIC)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE payment_collections (payment_id INTEGER PRIMARY KEY, source_type TEXT, source_sheet_row INTEGER, cost_center TEXT, payment_date TEXT, payment_timestamp TEXT, order_number TEXT, amount NUMERIC, bank_row_id TEXT)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO vw_orders VALUES "
                "('CC1', 'ORD1', '2026-05-01', 100), "
                "('CC1', 'ORD2', '2026-05-01', 200), "
                "('CC2', 'ORD1', '2026-05-01', 300)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO sales VALUES "
                "('CC1', 'ORD1', 100), "
                "('CC1', 'ORD2', 200), "
                "('CC2', 'ORD1', 300)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO payment_collections VALUES "
                "(1, 'google_sheet', 11, 'CC1', '2026-05-01', '2026-05-01 10:00:00', 'ORD1', 100, NULL), "
                "(2, 'google_sheet', 12, 'CC1', '2026-04-30', '2026-04-30 10:00:00', 'ORD2', 200, NULL)"
            )
        )
    engine.dispose()

    import app.reports.payment_evidence_review as review_module

    captured: dict[str, list[dict[str, object]]] = {}
    original_builder = review_module.build_payment_evidence_audit_rows

    def capturing_builder(
        *, order_rows, sales_rows=(), payment_evidence_rows=(), **kwargs
    ):
        captured["order_rows"] = list(order_rows)
        captured["sales_rows"] = list(sales_rows)
        captured["payment_evidence_rows"] = list(payment_evidence_rows)
        return original_builder(
            order_rows=order_rows,
            sales_rows=sales_rows,
            payment_evidence_rows=payment_evidence_rows,
            **kwargs,
        )

    monkeypatch.setattr(
        review_module, "build_payment_evidence_audit_rows", capturing_builder
    )

    async with session_scope(database_url) as session:
        rows = await fetch_payment_evidence_review_rows(
            session,
            PaymentEvidenceReviewFilters(
                source_type="google_sheet",
                cost_center="CC1",
                start_date="2026-05-01",
                limit=1,
            ),
        )

    assert [row["payment_id"] for row in rows] == [1]
    assert rows[0]["reconciliation_result"] == "paid"
    assert rows[0]["order_amount"] == 100
    assert rows[0]["payment_received"] == 100
    assert [
        (row["cost_center"], row["order_number"])
        for row in captured["payment_evidence_rows"]
    ] == [("CC1", "ORD1")]
    assert [
        (row["cost_center"], row["order_number"]) for row in captured["order_rows"]
    ] == [("CC1", "ORD1")]
    assert [
        (row["cost_center"], row["order_number"]) for row in captured["sales_rows"]
    ] == [("CC1", "ORD1")]
