from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
from typing import Iterable, Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas as pdfcanvas
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


def _normalize_agent_name(agent_name: str) -> str:
    """Normalize agent names for safe filenames by replacing spaces with underscores."""

    normalized = re.sub(r"\s+", "_", agent_name.strip())
    normalized = normalized.replace("/", "_").replace("\\", "_")
    return normalized or "agent"


def _compute_field_layout(page_width: float, left_margin: float, right_margin: float):
    weights = [
        0.16,  # Order No
        0.16,  # Order Date
        0.12,  # Value
        0.18,  # Payment Mode
        0.16,  # Payment Amt
        0.22,  # Remarks
    ]

    usable_width = page_width - left_margin - right_margin
    gap_px = 4
    num_fields = len(weights)
    total_gap_width = gap_px * (num_fields - 1)
    fields_total_width = usable_width - total_gap_width

    field_widths: list[int] = []
    remaining_width = fields_total_width

    for idx, weight in enumerate(weights):
        if idx < len(weights) - 1:
            width_i = int(fields_total_width * weight)
            field_widths.append(width_i)
            remaining_width -= width_i
        else:
            field_widths.append(int(remaining_width))

    original_widths = field_widths.copy()
    recovered_width = 0

    for i in range(min(5, len(field_widths))):
        halved_width = int(original_widths[i] * 0.5)
        recovered_width += original_widths[i] - halved_width
        field_widths[i] = halved_width

    if field_widths:
        field_widths[-1] += recovered_width

    x_positions: list[float] = []
    x = left_margin
    for width in field_widths:
        x_positions.append(x)
        x += width + gap_px

    return x_positions, field_widths


def _render_pdf(rows: list[_AssignmentRow], base_dir: Path) -> Path:
    first = rows[0]
    target_dir = base_dir / "leads_assignment" / first.batch_date.strftime("%Y-%m")
    target_dir.mkdir(parents=True, exist_ok=True)

    assignment_date_display = first.batch_date.strftime("%d-%m-%Y")
    agent_slug = _normalize_agent_name(first.agent_name)
    file_name = f"lead_{agent_slug}_{assignment_date_display}.pdf"
    file_path = target_dir / file_name

    page_width, page_height = landscape(A4)
    left_margin = 36
    right_margin = 36
    top_margin = 36
    bottom_margin = 36
    line_height = 14
    gap_row1_row2 = 5
    gap_row2_row3 = 3
    gap_between_leads = 8
    field_height = 16
    checkbox_size = 12

    x_positions, field_widths = _compute_field_layout(page_width, left_margin, right_margin)

    canvas = pdfcanvas.Canvas(str(file_path), pagesize=landscape(A4))

    def draw_header() -> float:
        canvas.setFont("Helvetica-Bold", 14)
        header_y = page_height - top_margin
        canvas.drawString(left_margin, header_y, f"Page Group Code: {first.page_group_code}")

        canvas.setFont("Helvetica", 11)
        header_y -= 16
        canvas.drawString(left_margin, header_y, f"{first.agent_code} - {first.agent_name}")

        header_y -= 16
        canvas.drawString(left_margin, header_y, f"Batch Date: {assignment_date_display}")

        header_y -= 16
        return header_y

    current_y = draw_header()

    for row in rows:
        required_height = (
            line_height
            + gap_row1_row2
            + line_height
            + gap_row2_row3
            + field_height
            + gap_between_leads
        )
        if current_y < bottom_margin + required_height:
            canvas.showPage()
            current_y = draw_header()

        lead_date_str = row.lead_date.strftime("%Y-%m-%d") if row.lead_date else ""
        lead_line = (
            f"ID: {row.rowid} - {lead_date_str} - {row.lead_type or ''} - "
            f"{row.mobile_number} - {row.cx_name or ''}"
        )

        canvas.setFont("Helvetica", 10)
        canvas.drawString(left_margin, current_y, lead_line)
        current_y -= line_height + gap_row1_row2

        canvas.setFont("Helvetica", 9)
        canvas.drawString(x_positions[0], current_y, "Order No")
        canvas.drawString(x_positions[1], current_y, "Order Date")
        canvas.drawString(x_positions[2], current_y, "Value")
        canvas.drawString(x_positions[3], current_y, "Payment Mode")
        canvas.drawString(x_positions[4], current_y, "Payment Amt")
        canvas.drawString(x_positions[5], current_y, "Remarks")
        current_y -= line_height + gap_row2_row3

        field_y = current_y
        form = canvas.acroForm

        conv_checkbox_x = max(left_margin - (checkbox_size + 36), 2)
        form.checkbox(
            name=f"conv_{row.rowid}",
            tooltip="Conv (Y/N)",
            x=conv_checkbox_x,
            y=field_y,
            size=checkbox_size,
            borderColor=colors.black,
            fillColor=None,
            textColor=colors.black,
        )
        canvas.drawString(conv_checkbox_x + checkbox_size + 2, field_y, "Conv (Y/N)")

        form.textfield(
            name=f"order_no_{row.rowid}",
            tooltip="Order No",
            x=x_positions[0],
            y=field_y,
            width=field_widths[0],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        form.textfield(
            name=f"order_date_{row.rowid}",
            tooltip="Order Date",
            x=x_positions[1],
            y=field_y,
            width=field_widths[1],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        form.textfield(
            name=f"value_{row.rowid}",
            tooltip="Value",
            x=x_positions[2],
            y=field_y,
            width=field_widths[2],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        form.textfield(
            name=f"payment_mode_{row.rowid}",
            tooltip="Payment Mode",
            x=x_positions[3],
            y=field_y,
            width=field_widths[3],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        form.textfield(
            name=f"payment_amt_{row.rowid}",
            tooltip="Payment Amt",
            x=x_positions[4],
            y=field_y,
            width=field_widths[4],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        form.textfield(
            name=f"remarks_{row.rowid}",
            tooltip="Remarks",
            x=x_positions[5],
            y=field_y,
            width=field_widths[5],
            height=field_height,
            borderStyle="underlined",
            borderColor=colors.black,
            textColor=colors.black,
        )

        current_y -= field_height + gap_between_leads

    canvas.save()

    return file_path


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
