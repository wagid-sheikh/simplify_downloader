from __future__ import annotations

import ast
from pathlib import Path

_BASE_BUSINESS_DECISION_CATEGORIES = {
    "WRITE_OFF_FULL",
    "WRITE_OFF_BALANCE",
    "RETURNED",
}
_AUTO_RECOVERY_CATEGORY = "PAYMENT_PROOF_AUTO_RECOVERED"


def _migration_assignments(filename: str) -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
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
    assignments = _migration_assignments("0106_recovery_categories.py")
    assert _BASE_BUSINESS_DECISION_CATEGORIES.issubset(
        set(assignments["_RECOVERY_CATEGORY_VALUES"])
    )


def test_active_recovery_category_constraint_includes_auto_recovered_value() -> None:
    assignments = _migration_assignments("0124_auto_recovered_category.py")
    category_values = set(assignments["_RECOVERY_CATEGORY_VALUES"])

    assert _BASE_BUSINESS_DECISION_CATEGORIES.issubset(category_values)
    assert _AUTO_RECOVERY_CATEGORY in category_values


def test_auto_recovered_category_migration_extends_current_head() -> None:
    assignments = _migration_assignments("0124_auto_recovered_category.py")
    assert assignments["revision"] == "0124_auto_recovered_category"
    assert assignments["down_revision"] == "0123_oli_rebuild_progress"
