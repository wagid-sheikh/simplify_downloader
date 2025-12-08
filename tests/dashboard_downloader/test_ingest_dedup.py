from datetime import date

import pytest

from app.common.ingest import service


class _FakeResult:
    def __init__(self, rowcount: int):
        self.rowcount = rowcount


class _FakeSession:
    def __init__(self):
        self.statements = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        return _FakeResult(len(getattr(stmt, "_values", [])))


class _FakeInsert:
    class _Excluded:
        def __getitem__(self, item):
            return f"excluded_{item}"

    def __init__(self, model):
        self.model = model
        self._values = []
        self.excluded = self._Excluded()
        self.conflict_action = None

    def values(self, values):
        self._values = values
        return self

    def on_conflict_do_nothing(self, *, index_elements):
        self.conflict_action = "do_nothing"
        self.index_elements = index_elements
        return self

    def on_conflict_do_update(self, *, index_elements, set_, where=None):
        self.conflict_action = "do_update"
        self.index_elements = index_elements
        self.set_ = set_
        self.where = where
        return self


@pytest.mark.asyncio
async def test_upsert_rows_dedupes_repeat_customers(monkeypatch):
    captured_values = []

    def fake_insert(model):
        stub = _FakeInsert(model)
        original_values = stub.values

        def capture(values):
            captured_values.append(values)
            return original_values(values)

        stub.values = capture  # type: ignore[assignment]
        return stub

    monkeypatch.setattr(service, "insert", fake_insert)

    newer_run = date(2024, 2, 2)
    rows = [
        {
            "store_code": "A001",
            "mobile_no": "12345",
            "status": "Yes",
            "run_id": "old",
            "run_date": date(2024, 2, 1),
        },
        {
            "store_code": "A001",
            "mobile_no": "12345",
            "status": "No",
            "run_id": "new",
            "run_date": newer_run,
        },
    ]

    result = await service._upsert_rows(_FakeSession(), "repeat_customers", rows)

    assert result == {"affected_rows": 1, "deduped_rows": 1}
    assert captured_values[-1] == [
        {
            "store_code": "A001",
            "mobile_no": "12345",
            "status": "No",
            "run_id": "new",
            "run_date": newer_run,
        }
    ]


@pytest.mark.asyncio
async def test_upsert_rows_dedupes_nonpackage_orders(monkeypatch):
    captured_values = []

    def fake_insert(model):
        stub = _FakeInsert(model)
        original_values = stub.values

        def capture(values):
            captured_values.append(values)
            return original_values(values)

        stub.values = capture  # type: ignore[assignment]
        return stub

    monkeypatch.setattr(service, "insert", fake_insert)

    recent_run = date(2024, 3, 3)
    rows = [
        {
            "store_code": "B002",
            "store_name": "Store B",
            "mobile_no": "99999",
            "taxable_amount": 10.0,
            "order_date": date(2024, 3, 1),
            "expected_delivery_date": date(2024, 3, 2),
            "actual_delivery_date": None,
            "run_id": "earlier",
            "run_date": date(2024, 3, 2),
        },
        {
            "store_code": "B002",
            "store_name": "Store B",
            "mobile_no": "99999",
            "taxable_amount": 20.0,
            "order_date": date(2024, 3, 2),
            "expected_delivery_date": date(2024, 3, 3),
            "actual_delivery_date": date(2024, 3, 3),
            "run_id": "later",
            "run_date": recent_run,
        },
    ]

    result = await service._upsert_rows(_FakeSession(), "nonpackage_all", rows)

    assert result == {"affected_rows": 1, "deduped_rows": 1}
    assert captured_values[-1] == [
        {
            "store_code": "B002",
            "store_name": "Store B",
            "mobile_no": "99999",
            "taxable_amount": 20.0,
            "order_date": date(2024, 3, 2),
            "expected_delivery_date": date(2024, 3, 3),
            "actual_delivery_date": date(2024, 3, 3),
            "run_id": "later",
            "run_date": recent_run,
        }
    ]


@pytest.mark.asyncio
async def test_upsert_rows_dedupes_undelivered_orders(monkeypatch):
    captured_values = []

    def fake_insert(model):
        stub = _FakeInsert(model)
        original_values = stub.values

        def capture(values):
            captured_values.append(values)
            return original_values(values)

        stub.values = capture  # type: ignore[assignment]
        return stub

    monkeypatch.setattr(service, "insert", fake_insert)

    newer_run = date(2024, 4, 2)
    rows = [
        {
            "store_code": "C003",
            "store_name": "Store C",
            "order_id": "ORD-1",
            "taxable_amount": 50.0,
            "net_amount": 75.0,
            "service_code": "svc",
            "mobile_no": "88888",
            "status": "pending",
            "customer_id": "cust1",
            "order_date": date(2024, 4, 1),
            "expected_deliver_on": date(2024, 4, 2),
            "actual_deliver_on": None,
            "run_id": "early",
            "run_date": date(2024, 4, 1),
        },
        {
            "store_code": "C003",
            "store_name": "Store C",
            "order_id": "ORD-1",
            "taxable_amount": 60.0,
            "net_amount": 80.0,
            "service_code": "svc",
            "mobile_no": "88888",
            "status": "delivered",
            "customer_id": "cust1",
            "order_date": date(2024, 4, 2),
            "expected_deliver_on": date(2024, 4, 3),
            "actual_deliver_on": date(2024, 4, 2),
            "run_id": "later",
            "run_date": newer_run,
        },
    ]

    result = await service._upsert_rows(_FakeSession(), "undelivered_all", rows)

    assert result == {"affected_rows": 1, "deduped_rows": 1}
    assert captured_values[-1] == [
        {
            "store_code": "C003",
            "store_name": "Store C",
            "order_id": "ORD-1",
            "taxable_amount": 60.0,
            "net_amount": 80.0,
            "service_code": "svc",
            "mobile_no": "88888",
            "status": "delivered",
            "customer_id": "cust1",
            "order_date": date(2024, 4, 2),
            "expected_deliver_on": date(2024, 4, 3),
            "actual_deliver_on": date(2024, 4, 2),
            "run_id": "later",
            "run_date": newer_run,
        }
    ]


@pytest.mark.asyncio
async def test_upsert_rows_prefers_order_placed_for_missed_leads(monkeypatch):
    captured_values = []

    def fake_insert(model):
        stub = _FakeInsert(model)
        original_values = stub.values

        def capture(values):
            captured_values.append(values)
            return original_values(values)

        stub.values = capture  # type: ignore[assignment]
        return stub

    monkeypatch.setattr(service, "insert", fake_insert)

    rows = [
        {
            "store_code": "D004",
            "mobile_number": "77777",
            "pickup_row_id": 1,
            "pickup_created_date": date(2024, 5, 2),
            "pickup_created_time": "12:00",
            "run_id": "recent_no_order",
            "run_date": date(2024, 5, 2),
            "is_order_placed": False,
        },
        {
            "store_code": "D004",
            "mobile_number": "77777",
            "pickup_row_id": 2,
            "pickup_created_date": date(2024, 5, 1),
            "pickup_created_time": "10:00",
            "run_id": "earlier_order_placed",
            "run_date": date(2024, 5, 1),
            "is_order_placed": True,
        },
    ]

    result = await service._upsert_rows(_FakeSession(), "missed_leads", rows)

    assert result == {"affected_rows": 1, "deduped_rows": 1}
    assert captured_values[-1] == [
        {
            "store_code": "D004",
            "mobile_number": "77777",
            "pickup_row_id": 2,
            "pickup_created_date": date(2024, 5, 1),
            "pickup_created_time": "10:00",
            "run_id": "earlier_order_placed",
            "run_date": date(2024, 5, 1),
            "is_order_placed": True,
        }
    ]
