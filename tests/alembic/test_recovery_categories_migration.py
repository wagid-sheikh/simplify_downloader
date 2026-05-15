from __future__ import annotations

import ast
from pathlib import Path


_REQUIRED_BUSINESS_DECISION_CATEGORIES = {
    "WRITE_OFF_FULL",
    "WRITE_OFF_BALANCE",
    "RETURNED",
}


def _migration_assignments() -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0106_recovery_categories.py"
    tree = ast.parse(module_path.read_text())
    assignments: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                try:
                    assignments[target.id] = ast.literal_eval(node.value)
                except ValueError:
                    continue
    return assignments


def test_recovery_category_constraint_includes_business_decision_values() -> None:
    assignments = _migration_assignments()
    assert _REQUIRED_BUSINESS_DECISION_CATEGORIES.issubset(
        set(assignments["_RECOVERY_CATEGORY_VALUES"])
    )


def test_recovery_category_migration_extends_current_head() -> None:
    assignments = _migration_assignments()
    assert assignments["revision"] == "0106_recovery_categories"
    assert assignments["down_revision"] == "0105_missing_pay_vw_orders"
