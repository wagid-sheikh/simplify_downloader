import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.common.ingest import service


def _make_missed_lead_row(row_id: int) -> dict:
    return {
        "pickup_row_id": row_id,
        "mobile_number": f"{row_id:04d}",
        "pickup_no": None,
        "pickup_created_date": None,
        "pickup_created_time": None,
        "store_code": "A001",
        "store_name": None,
        "pickup_date": None,
        "pickup_time": None,
        "customer_name": None,
        "special_instruction": None,
        "source": None,
        "final_source": None,
        "customer_type": None,
        "is_order_placed": None,
        "run_id": "test-run",
        "run_date": date(2025, 1, 1),
    }


@pytest.mark.asyncio
async def test_upsert_batch_uses_multiple_chunks(monkeypatch):
    batch_size = service.BULK_INSERT_BATCH_SIZE
    total_rows = batch_size * 2 + batch_size // 2
    rows = [_make_missed_lead_row(i) for i in range(total_rows)]

    call_sizes: list[int] = []

    async def fake_upsert_rows(session, bucket, chunk):
        call_sizes.append(len(chunk))
        return {"affected_rows": len(chunk), "deduped_rows": len(chunk)}

    monkeypatch.setattr(service, "_upsert_rows", fake_upsert_rows)

    totals = await service._upsert_batch(None, "missed_leads", rows)

    assert totals == {"affected_rows": total_rows, "deduped_rows": total_rows}
    assert call_sizes == [batch_size, batch_size, total_rows - batch_size * 2]
