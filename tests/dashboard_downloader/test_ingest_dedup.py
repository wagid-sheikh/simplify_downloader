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

    def values(self, values):
        self._values = values
        return self

    def on_conflict_do_update(self, *, index_elements, set_):
        self.index_elements = index_elements
        self.set_ = set_
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

    assert result == 1
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

    assert result == 1
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
