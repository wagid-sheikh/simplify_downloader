import importlib.util
from pathlib import Path

import pytest


pytest.importorskip("reportlab")
from reportlab.pdfgen import canvas as rl_canvas


_MODULE_PATH = Path(__file__).resolve().parents[2] / "app" / "lead_assignment" / "pdf_generator.py"
_MODULE_SPEC = importlib.util.spec_from_file_location(
    "_pdf_generator", _MODULE_PATH
)
_MODULE = importlib.util.module_from_spec(_MODULE_SPEC)
assert _MODULE_SPEC and _MODULE_SPEC.loader  # for mypy
_MODULE_SPEC.loader.exec_module(_MODULE)

_FormTable = _MODULE._FormTable


def test_form_table_split_propagates_input_rows():
    data = [
        ["header"],
        ["row1"],
        ["input1"],
        ["row2"],
        ["input2"],
    ]

    table = _FormTable(data, input_rows={2, 4}, rowHeights=[10] * len(data))

    split_tables = table.split(availWidth=200, availHeight=25)

    assert len(split_tables) == 2
    assert isinstance(split_tables[0], _FormTable)

    assert split_tables[0]._input_rows == set()
    assert split_tables[1]._input_rows == {0, 2}


def test_form_table_split_handles_slice_row_slices():
    data = [
        ["header"],
        ["row1"],
        ["input1"],
        ["row2"],
        ["input2"],
    ]

    table = _FormTable(data, input_rows={2, 4}, rowHeights=[10] * len(data))

    original_split_rows = table._splitRows

    def _split_rows_with_slices(avail_height: int):
        return [slice(start, end) for start, end in original_split_rows(avail_height)]

    table._splitRows = _split_rows_with_slices  # type: ignore[assignment]

    split_tables = table.split(availWidth=200, availHeight=25)

    assert len(split_tables) == 2
    assert isinstance(split_tables[0], _FormTable)

    assert split_tables[0]._input_rows == set()
    assert split_tables[1]._input_rows == {0, 2}


def test_form_table_drawon_uses_acroform_arguments(tmp_path):
    pdf_bytes = tmp_path / "form.pdf"
    canvas = rl_canvas.Canvas(str(pdf_bytes))

    data = [["header", "header2"], ["input1", "input2"]]
    table = _FormTable(
        data,
        input_rows={1},
        colWidths=[100, 100],
        rowHeights=[20, 20],
    )

    table.wrapOn(canvas, 200, 40)

    calls: list[dict] = []

    def capture_textfield(**kwargs):
        calls.append(kwargs)

    canvas.acroForm.textfield = capture_textfield  # type: ignore[assignment]

    table.drawOn(canvas, 0, 0)
    canvas.save()

    assert calls, "AcroForm text fields were not rendered"
    assert all("name" in call for call in calls)
    assert all("fieldName" not in call for call in calls)
