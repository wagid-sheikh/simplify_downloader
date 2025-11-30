import importlib.util
from pathlib import Path

import pytest


pytest.importorskip("reportlab")


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
