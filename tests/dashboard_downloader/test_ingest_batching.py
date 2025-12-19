import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.common.ingest import service
from app.dashboard_downloader.json_logger import JsonLogger


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


def test_load_csv_rows_counts_missing_mobile(tmp_path: Path):
    csv_path = tmp_path / "missed_leads.csv"
    csv_path.write_text(
        """Pickup Row Id,Store Code,Mobile Number
1,A001,
2,A001,
3,A001,9999
""",
        encoding="utf-8",
    )

    skip_counters: defaultdict[tuple[str, str], int] = defaultdict(int)

    rows = list(
        service._load_csv_rows(
            "missed_leads",
            csv_path,
            logger=None,
            row_context={"run_id": "test-run", "run_date": date(2024, 1, 1)},
            skip_counters=skip_counters,
        )
    )

    assert len(rows) == 1
    assert rows[0]["mobile_number"] == "9999"
    assert dict(skip_counters) == {("A001", "2024-01-01"): 2}


@pytest.mark.asyncio
async def test_ingest_bucket_logs_missing_mobile_summary(monkeypatch, tmp_path: Path):
    csv_path = tmp_path / "missed_leads.csv"
    csv_path.write_text(
        """Pickup Row Id,Store Code,Mobile Number
1,A001,
2,A001,
3,A001,9999
""",
        encoding="utf-8",
    )

    @asynccontextmanager
    async def fake_session_scope(database_url):
        class _DummySession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def begin(self):
                return self

        yield _DummySession()

    ingested_batches: list[list[dict]] = []

    async def fake_upsert_batch(session, bucket, rows):
        ingested_batches.append(rows)
        return {"affected_rows": len(rows), "deduped_rows": len(rows)}

    events: list[dict] = []

    def fake_log_event(**kwargs):
        events.append(kwargs)

    monkeypatch.setattr(service, "session_scope", fake_session_scope)
    monkeypatch.setattr(service, "_upsert_batch", fake_upsert_batch)
    monkeypatch.setattr(service, "log_event", fake_log_event)

    logger = JsonLogger(log_file_path=None)

    totals = await service.ingest_bucket(
        bucket="missed_leads",
        csv_path=csv_path,
        batch_size=10,
        database_url="sqlite+aiosqlite://",
        logger=logger,
        run_id="test-run",
        run_date=date(2024, 1, 1),
    )

    assert totals == {"rows": 1, "deduped_rows": 1}
    assert len(ingested_batches) == 1
    assert len(ingested_batches[0]) == 1
    ingested_row = ingested_batches[0][0]
    assert ingested_row["pickup_row_id"] == 3
    assert ingested_row["store_code"] == "A001"
    assert ingested_row["mobile_number"] == "9999"
    assert ingested_row["run_id"] == "test-run"
    assert ingested_row["run_date"] == date(2024, 1, 1)

    summary_events = [
        event
        for event in events
        if "skipped_missing_mobile" in event
        and "missing mobile_number" in event.get("message", "")
    ]
    assert summary_events, "expected summary event for missing mobile numbers"
    assert summary_events[0]["skipped_missing_mobile"] == [
        {"store_code": "A001", "report_date": "2024-01-01", "count": 2}
    ]
