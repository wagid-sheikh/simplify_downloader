import asyncio
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import pytest

from app.common.audit import audit_bucket
from app.common.cleanup import cleanup_bucket
from app.dashboard_downloader.json_logger import JsonLogger


class _DummySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def begin(self):
        return self


def test_deduped_ingest_allows_cleanup(tmp_path: Path, monkeypatch):
    pytest.importorskip("sqlalchemy")
    from app.common.ingest import service

    async def _run_test() -> None:
        merged_path = tmp_path / "merged_repeat_customers.csv"
        merged_path.write_text(
            """Store Code,Mobile No.,Status
A001,12345,Yes
A001,12345,No
""",
            encoding="utf-8",
        )

        @asynccontextmanager
        async def fake_session_scope(database_url):
            yield _DummySession()

        async def fake_upsert_batch(session, bucket, rows):
            spec = service.MERGE_BUCKET_DB_SPECS[bucket]
            deduped_rows = service._dedupe_rows(bucket, spec, rows)
            return {"affected_rows": len(deduped_rows), "deduped_rows": len(deduped_rows)}

        monkeypatch.setattr(service, "session_scope", fake_session_scope)
        monkeypatch.setattr(service, "_upsert_batch", fake_upsert_batch)

        logger = JsonLogger(log_file_path=None)
        ingest_totals = await service.ingest_bucket(
            bucket="repeat_customers",
            csv_path=merged_path,
            batch_size=10,
            database_url="sqlite+aiosqlite://",
            logger=logger,
            run_id="test-run",
            run_date=date(2024, 1, 1),
        )

        assert ingest_totals == {"rows": 1, "deduped_rows": 1}

        counts = {
            "download_total": 2,
            "merged_rows": ingest_totals["deduped_rows"],
            "raw_merged_rows": 2,
            "ingested_rows": ingest_totals["rows"],
        }

        audit_result = audit_bucket(bucket="repeat_customers", counts=counts, logger=logger)
        assert audit_result["status"] == "ok"

        store_path = tmp_path / "store.csv"
        store_path.write_text("dummy", encoding="utf-8")

        cleanup_bucket(
            bucket="repeat_customers",
            download_info={"S001": {"path": str(store_path)}, "__merged__": {"path": str(merged_path)}},
            merged_path=merged_path,
            audit_status=audit_result["status"],
            logger=logger,
        )

        assert not merged_path.exists()
        assert not store_path.exists()

    asyncio.run(_run_test())


def test_audit_allows_subset_ingest_in_single_session():
    logger = JsonLogger(log_file_path=None)
    counts = {
        "download_total": 120,
        "merged_rows": 120,
        "ingested_rows": 45,
    }

    result = audit_bucket(
        bucket="nonpackage_all", counts=counts, logger=logger, single_session=True
    )

    assert result["status"] == "ok"


def test_audit_warns_on_zero_ingest_in_single_session():
    logger = JsonLogger(log_file_path=None)
    counts = {"download_total": 20, "merged_rows": 20, "ingested_rows": 0}

    result = audit_bucket(
        bucket="undelivered_all", counts=counts, logger=logger, single_session=True
    )

    assert result["status"] == "warn"
