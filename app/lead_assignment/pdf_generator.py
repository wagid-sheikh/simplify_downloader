from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import config

__all__ = ["generate_pdfs_for_batch"]


@dataclass
class _AssignmentRow:
    assignment_batch_id: int
    batch_date: date
    store_code: str
    store_name: str | None
    agent_id: int
    agent_code: str
    agent_name: str
    page_group_code: str
    rowid: int
    lead_date: date | None
    lead_type: str | None
    mobile_number: str
    cx_name: str | None
    address: str | None
    lead_source: str | None


async def generate_pdfs_for_batch(
    db_session: AsyncSession, batch_id: int, *, reports_root: str | Path | None = None
) -> list[int]:
    """Render PDFs for a batch, persist them, and insert `documents` rows."""

    assignments = await _fetch_assignments(db_session, batch_id)
    if not assignments:
        return []

    base_dir = Path(reports_root or config.reports_root).resolve()

    grouped = _group_assignments(assignments)
    document_ids: list[int] = []
    for rows in grouped.values():
        file_path = _render_pdf(rows, base_dir)
        document_id = await _insert_document_record(db_session, rows[0], file_path)
        document_ids.append(document_id)

    return document_ids


async def _fetch_assignments(db_session: AsyncSession, batch_id: int) -> list[_AssignmentRow]:
    result = await db_session.execute(
        text(
            """
            SELECT
                la.assignment_batch_id,
                lab.batch_date,
                la.store_code,
                la.store_name,
                la.agent_id,
                am.agent_code,
                am.agent_name,
                la.page_group_code,
                la.rowid,
                la.lead_date,
                la.lead_type,
                la.mobile_number,
                la.cx_name,
                la.address,
                la.lead_source
            FROM lead_assignments la
            JOIN agents_master am ON am.id = la.agent_id
            JOIN lead_assignment_batches lab ON lab.id = la.assignment_batch_id
            WHERE la.assignment_batch_id = :batch_id
            ORDER BY la.store_code, la.agent_id, la.rowid
            """
        ),
        {"batch_id": batch_id},
    )

    return [
        _AssignmentRow(
            assignment_batch_id=row.assignment_batch_id,
            batch_date=row.batch_date,
            store_code=row.store_code,
            store_name=row.store_name,
            agent_id=row.agent_id,
            agent_code=row.agent_code,
            agent_name=row.agent_name,
            page_group_code=row.page_group_code,
            rowid=row.rowid,
            lead_date=row.lead_date,
            lead_type=row.lead_type,
            mobile_number=row.mobile_number,
            cx_name=row.cx_name,
            address=row.address,
            lead_source=row.lead_source,
        )
        for row in result
    ]


def _group_assignments(rows: Iterable[_AssignmentRow]) -> Mapping[tuple[str, int, int], list[_AssignmentRow]]:
    grouped: dict[tuple[str, int, int], list[_AssignmentRow]] = {}
    for row in rows:
        key = (row.store_code, row.agent_id, row.assignment_batch_id)
        grouped.setdefault(key, []).append(row)
    return grouped


def _render_pdf(rows: list[_AssignmentRow], base_dir: Path) -> Path:
    first = rows[0]
    target_dir = base_dir / "leads_assignment" / first.batch_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"leads_assignment_{first.batch_date.isoformat()}_{first.store_code}_{first.agent_code}.pdf"
    file_path = target_dir / file_name

    doc = SimpleDocTemplate(str(file_path), pagesize=A4, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    header_style = ParagraphStyle(
        "Header",
        parent=styles["Heading2"],
        fontSize=14,
        spaceAfter=4,
    )
    meta_style = ParagraphStyle(
        "Meta",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=2,
    )

    elements = [
        Paragraph(f"Page Group Code: {first.page_group_code}", header_style),
        Paragraph(f"Agent: {first.agent_code} - {first.agent_name}", meta_style),
        Paragraph(f"Batch Date: {first.batch_date.isoformat()}", meta_style),
        Spacer(1, 10),
    ]

    table_data = _table_data(rows)
    column_factors = [
        0.06,
        0.1,
        0.05,
        0.12,
        0.12,
        0.12,
        0.06,
        0.07,
        0.07,
        0.06,
        0.07,
        0.06,
        0.04,
    ]
    col_widths = [doc.width * factor for factor in column_factors]

    table = Table(table_data, repeatRows=1, colWidths=col_widths)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (0, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
            ]
        )
    )

    elements.append(table)
    doc.build(elements)

    return file_path


def _table_data(rows: Iterable[_AssignmentRow]) -> list[list[str]]:
    header = [
        "RowID",
        "Lead Date",
        "Type",
        "Mobile No",
        "Customer Name",
        "Address",
        "Conv (Y/N)",
        "Order No",
        "Order Date",
        "Value",
        "Payment Mode",
        "Payment Amt",
        "Remarks",
    ]

    table_rows: list[list[str]] = [header]
    for row in rows:
        snapshot = [
            str(row.rowid),
            row.lead_date.isoformat() if row.lead_date else "",
            row.lead_type or "",
            row.mobile_number,
            row.cx_name or "",
            row.address or "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        inputs = ["" for _ in header]
        table_rows.append(snapshot)
        table_rows.append(inputs)

    return table_rows


async def _insert_document_record(
    db_session: AsyncSession, row: _AssignmentRow, file_path: Path
) -> int:
    size_bytes = file_path.stat().st_size if file_path.exists() else None
    insert_sql = text(
        """
        INSERT INTO documents (
            doc_type,
            doc_subtype,
            doc_date,
            reference_name_1,
            reference_id_1,
            reference_name_2,
            reference_id_2,
            reference_name_3,
            reference_id_3,
            file_name,
            mime_type,
            file_size_bytes,
            storage_backend,
            file_path,
            file_blob,
            checksum,
            status,
            error_message,
            created_at,
            created_by
        ) VALUES (
            'leads_assignment',
            'per_store_agent_pdf',
            :doc_date,
            'pipeline',
            'leads_assignment',
            'store_code',
            :store_code,
            'agent_code',
            :agent_code,
            :file_name,
            'application/pdf',
            :file_size_bytes,
            'fs',
            :file_path,
            NULL,
            NULL,
            'ok',
            NULL,
            :created_at,
            'leads_assignment_pipeline'
        )
        RETURNING id
        """
    )

    result = await db_session.execute(
        insert_sql,
        {
            "doc_date": row.batch_date,
            "store_code": row.store_code,
            "agent_code": row.agent_code,
            "file_name": file_path.name,
            "file_size_bytes": size_bytes,
            "file_path": str(file_path),
            "created_at": datetime.now(timezone.utc),
        },
    )

    return int(result.scalar_one())
