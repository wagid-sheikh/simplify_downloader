from __future__ import annotations

import shutil
import sqlite3
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pytest
pdfplumber = pytest.importorskip("pdfplumber")
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.common.db import session_scope
from app.config import config
from app.lead_assignment.assigner import _insert_assignments
from app.lead_assignment.pipeline import run_leads_assignment_pipeline


def _db_path() -> Path:
    parsed = urlparse(config.database_url)
    return Path(parsed.path)


def _reset_lead_assignment_schema(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    tables_to_drop = [
        "lead_assignment_outcomes",
        "lead_assignments",
        "lead_assignment_batches",
        "store_lead_assignment_map",
        "agents_master",
        "store_master",
        "notification_recipients",
        "email_templates",
        "notification_profiles",
        "pipelines",
        "documents",
        "missed_leads",
    ]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS store_master (
            store_code TEXT PRIMARY KEY,
            store_name TEXT,
            assign_leads BOOLEAN NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS agents_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_code CHAR(4) UNIQUE,
            agent_name TEXT NOT NULL,
            mobile_number TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS store_lead_assignment_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_code TEXT NOT NULL,
            agent_id INTEGER NOT NULL,
            is_enabled BOOLEAN NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            max_existing_per_lot INTEGER,
            max_new_per_lot INTEGER,
            max_daily_leads INTEGER,
            UNIQUE (store_code, agent_id)
        );

        CREATE TABLE IF NOT EXISTS lead_assignment_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_date DATE NOT NULL,
            triggered_by TEXT,
            run_id TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS lead_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_batch_id INTEGER NOT NULL,
            lead_id INTEGER NOT NULL,
            agent_id INTEGER NOT NULL,
            page_group_code TEXT NOT NULL,
            rowid INTEGER NOT NULL,
            lead_assignment_code TEXT NOT NULL,
            store_code TEXT NOT NULL,
            store_name TEXT,
            lead_date DATE,
            lead_type CHAR(1),
            mobile_number TEXT NOT NULL,
            cx_name TEXT,
            address TEXT,
            lead_source TEXT,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (lead_assignment_code),
            UNIQUE (lead_id)
        );

        CREATE TABLE IF NOT EXISTS lead_assignment_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_assignment_id INTEGER NOT NULL,
            converted_flag BOOLEAN,
            order_number TEXT,
            order_date DATE,
            order_value NUMERIC(12, 2),
            payment_mode TEXT,
            payment_amount NUMERIC(12, 2),
            remarks TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (lead_assignment_id)
        );

        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_type TEXT,
            doc_subtype TEXT,
            doc_date DATE,
            reference_name_1 TEXT,
            reference_id_1 TEXT,
            reference_name_2 TEXT,
            reference_id_2 TEXT,
            reference_name_3 TEXT,
            reference_id_3 TEXT,
            file_name TEXT,
            mime_type TEXT,
            file_size_bytes INTEGER,
            storage_backend TEXT,
            file_path TEXT,
            file_blob BLOB,
            checksum TEXT,
            status TEXT,
            error_message TEXT,
            created_at DATETIME,
            created_by TEXT
        );

        CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS notification_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER,
            code TEXT,
            description TEXT,
            env TEXT,
            scope TEXT,
            attach_mode TEXT,
            is_active BOOLEAN DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS email_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            name TEXT,
            subject_template TEXT,
            body_template TEXT,
            is_active BOOLEAN DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS notification_recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            store_code TEXT,
            email_address TEXT,
            display_name TEXT,
            send_as TEXT,
            env TEXT,
            is_active BOOLEAN DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS missed_leads (
            pickup_row_id INTEGER PRIMARY KEY,
            mobile_number TEXT NOT NULL,
            pickup_no TEXT,
            pickup_created_date DATE,
            pickup_created_time TEXT,
            store_code TEXT NOT NULL,
            store_name TEXT,
            pickup_date DATE,
            pickup_time TEXT,
            customer_name TEXT,
            special_instruction TEXT,
            source TEXT,
            final_source TEXT,
            customer_type TEXT,
            is_order_placed BOOLEAN,
            run_id TEXT,
            run_date DATE,
            lead_assigned BOOLEAN NOT NULL DEFAULT 0,
            UNIQUE (store_code, mobile_number)
        );
        """
    )

    conn.commit()
    conn.close()


def _seed_data(db_path: Path) -> dict[str, object]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO store_master (store_code, store_name, assign_leads) VALUES (?, ?, 1)",
        ("S001", "Test Store",),
    )

    cursor.execute(
        "INSERT INTO agents_master (agent_code, agent_name, mobile_number) VALUES (?, ?, ?)",
        ("0001", "Primary Agent", "9999999999"),
    )
    agent_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO store_lead_assignment_map (
            store_code, agent_id, max_existing_per_lot, max_new_per_lot, max_daily_leads
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("S001", agent_id, 1, 1, 2),
    )

    cursor.execute(
        "INSERT INTO pipelines (code, description) VALUES (?, ?)",
        ("leads_assignment", "Leads Assignment"),
    )
    pipeline_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO notification_profiles (
            pipeline_id, code, description, env, scope, attach_mode, is_active
        ) VALUES (?, ?, ?, 'any', 'store', 'per_store_pdf', 1)
        """,
        (pipeline_id, "leads_assignment", "Assign leads"),
    )
    profile_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO email_templates (profile_id, name, subject_template, body_template, is_active)
        VALUES (?, 'default', 'Leads for {{ store_code }}', 'Attachments ready', 1)
        """,
        (profile_id,),
    )

    cursor.execute(
        """
        INSERT INTO notification_recipients (
            profile_id, store_code, email_address, display_name, send_as, env, is_active
        ) VALUES (?, 'S001', 'agent@example.com', 'Agent One', 'to', 'any', 1)
        """,
        (profile_id,),
    )

    base_date = date(2024, 1, 15)
    leads = [
        (1, "9000000001", None, base_date, "09:00", "S001", "Test Store", base_date, "09:00", "Existing CX", "Doorstep", "web", "web", "Existing", 0, None, base_date, 0),
        (2, "9000000002", None, base_date, "09:05", "S001", "Test Store", base_date, "09:05", "New CX", "Pickup", "app", "app", "New", 0, None, base_date, 0),
        (3, "9000000003", None, base_date, "09:10", "S001", "Test Store", base_date, "09:10", "New CX 2", "Pickup", "app", "app", "New", 0, None, base_date, 0),
    ]
    cursor.executemany(
        """
        INSERT INTO missed_leads (
            pickup_row_id, mobile_number, pickup_no, pickup_created_date, pickup_created_time,
            store_code, store_name, pickup_date, pickup_time, customer_name, special_instruction,
            source, final_source, customer_type, is_order_placed, run_id, run_date, lead_assigned
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        leads,
    )

    conn.commit()
    conn.close()

    return {"agent_id": agent_id, "lead_ids": [lead[0] for lead in leads]}


@pytest.fixture()
def prepared_db(monkeypatch) -> dict[str, object]:
    db_path = _db_path()

    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES},
    )

    class _AsyncSessionWrapper:
        def __init__(self, sync_session: Session):
            self._session = sync_session

        async def execute(self, statement, params=None):
            return self._session.execute(statement, params or {})

        async def commit(self):
            return self._session.commit()

        def begin(self):
            if self._session.in_transaction():
                self._session.commit()

            sync_ctx = self._session.begin()

            class _AsyncBegin:
                async def __aenter__(self_nonlocal):
                    sync_ctx.__enter__()
                    return self

                async def __aexit__(self_nonlocal, exc_type, exc, tb):
                    sync_ctx.__exit__(exc_type, exc, tb)

            return _AsyncBegin()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self._session.close()

    @asynccontextmanager
    async def _patched_session_scope(_url: str):
        sync_session = Session(engine)
        async_session = _AsyncSessionWrapper(sync_session)
        try:
            yield async_session
        finally:
            sync_session.close()

    monkeypatch.setattr("app.lead_assignment.pipeline.session_scope", _patched_session_scope)
    monkeypatch.setattr(
        "app.lead_assignment.pdf_generator.session_scope",
        _patched_session_scope,
        raising=False,
    )
    monkeypatch.setattr("app.common.db.session_scope", _patched_session_scope, raising=False)
    globals()["session_scope"] = _patched_session_scope

    reports_dir = Path(config.reports_root)
    shutil.rmtree(reports_dir / "leads_assignment", ignore_errors=True)
    _reset_lead_assignment_schema(db_path)
    state = _seed_data(db_path)
    return state


@pytest.mark.asyncio()
async def test_lead_assignment_pipeline_end_to_end(monkeypatch, prepared_db):
    sent_emails: list = []

    def _fake_send_email(config_obj, plan):
        sent_emails.append(plan)
        return True

    monkeypatch.setattr("app.lead_assignment.pipeline._send_email", _fake_send_email)

    await run_leads_assignment_pipeline(run_id="test-batch")

    async with session_scope(config.database_url) as session:
        assignment_count = int(
            (await session.execute(text("SELECT COUNT(*) FROM lead_assignments"))).scalar_one()
        )
        batch_count = int(
            (await session.execute(text("SELECT COUNT(*) FROM lead_assignment_batches"))).scalar_one()
        )
        assigned_flags = dict(
            (await session.execute(
                text(
                    "SELECT pickup_row_id, lead_assigned FROM missed_leads ORDER BY pickup_row_id"
                )
            )).all()
        )
        documents_rows = (await session.execute(text("SELECT id, file_path FROM documents"))).all()

    assert batch_count == 1
    assert assignment_count == 2  # third lead exceeds caps
    assert assigned_flags == {1: 1, 2: 1, 3: 0}

    assert len(documents_rows) == 1
    pdf_path = Path(documents_rows[0].file_path)
    assert pdf_path.exists()

    with pdfplumber.open(pdf_path) as pdf:
        text_content = "\n".join(page.extract_text() or "" for page in pdf.pages)
    assert "RowID" in text_content
    assert "9000000001" in text_content
    assert "9000000002" in text_content

    assert len(sent_emails) == 1
    assert sent_emails[0].attachments and sent_emails[0].attachments[0] == pdf_path

    conn = sqlite3.connect(_db_path())
    conn.execute("UPDATE missed_leads SET lead_assigned = 1")
    conn.commit()
    conn.close()

    sent_emails.clear()
    await run_leads_assignment_pipeline(run_id="test-batch-repeat")

    async with session_scope(config.database_url) as session:
        new_batch_count = int(
            (await session.execute(text("SELECT COUNT(*) FROM lead_assignment_batches"))).scalar_one()
        )
        total_assignments = int(
            (await session.execute(text("SELECT COUNT(*) FROM lead_assignments"))).scalar_one()
        )
        new_documents = int(
            (await session.execute(text("SELECT COUNT(*) FROM documents"))).scalar_one()
        )

    assert new_batch_count == 2
    assert total_assignments == assignment_count
    assert new_documents == 1
    assert sent_emails == []


@pytest.mark.asyncio()
async def test_insert_assignments_is_idempotent(monkeypatch, prepared_db):
    assignments = [
        {
            "assignment_batch_id": 1,
            "lead_id": 10,
            "agent_id": 1,
            "page_group_code": "PAG",
            "rowid": 1,
            "lead_assignment_code": "PAG-0001",
            "store_code": "S001",
            "store_name": "Test Store",
            "lead_date": date.today(),
            "lead_type": "E",
            "mobile_number": "9000000000",
            "cx_name": "Test Customer",
            "address": "",
            "lead_source": "source",
        }
    ]

    async with session_scope(config.database_url) as session:
        first_insert = await _insert_assignments(session, assignments)
        duplicate_insert = await _insert_assignments(session, assignments)
        total_count = int(
            (await session.execute(text("SELECT COUNT(*) FROM lead_assignments"))).scalar_one()
        )

    assert first_insert == 1
    assert duplicate_insert == 0
    assert total_count == 1
