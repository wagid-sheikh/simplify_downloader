"""Mark audit payment actions."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "0116_audit_action_status"
down_revision = "0115_canon_missing_view"
branch_labels = None
depends_on = None

_ACTION_STATUS_SQL = """
CASE
    WHEN (',' || replace(upper(COALESCE(base.recovery_statuses_csv, '')), ' ', '') || ',') LIKE '%,TO_BE_RECOVERED,%'
      OR (',' || replace(upper(COALESCE(base.recovery_statuses_csv, '')), ' ', '') || ',') LIKE '%,TO_BE_COMPENSATED,%'
      OR (',' || replace(upper(COALESCE(base.recovery_statuses_csv, '')), ' ', '') || ',') LIKE '%,RECOVERED,%'
      OR (',' || replace(upper(COALESCE(base.recovery_statuses_csv, '')), ' ', '') || ',') LIKE '%,COMPENSATED,%'
      OR (',' || replace(upper(COALESCE(base.recovery_statuses_csv, '')), ' ', '') || ',') LIKE '%,WRITE_OFF,%'
        THEN 'non_actionable_recovery_status'
    WHEN lower(COALESCE(base.reconciliation_result, '')) IN ('short', 'grouped short')
        THEN 'actionable_short_payment'
    ELSE 'audit_only'
END AS operator_actionable_payment_status
""".strip()


def _load_payment_audit_sql() -> tuple[str, str]:
    """Load the prior canonical audit view SQL without editing history."""

    previous_path = Path(__file__).with_name("0114_payment_audit_canon.py")
    spec = importlib.util.spec_from_file_location(
        "v0114_payment_audit_canon", previous_path
    )
    if (
        spec is None or spec.loader is None
    ):  # pragma: no cover - defensive migration guard
        raise RuntimeError("Unable to load 0114_payment_audit_canon.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.POSTGRES_VIEW_SQL, module.SQLITE_VIEW_SQL


def _wrap_view_sql(view_sql: str, *, qualified_view_name: str) -> str:
    """Add the operator action marker while preserving audit classification columns.

    ``reconciliation_result`` remains a raw audit classification for
    ``vw_payment_evidence_reconciliation``. The derived
    ``operator_actionable_payment_status`` column documents whether the row is a
    Daily Sales Short Payments action candidate; rows with recovery workflow
    statuses are non-actionable even if their audit classification is short.
    """

    header = f"CREATE OR REPLACE VIEW {qualified_view_name} AS"
    sqlite_header = f"CREATE VIEW {qualified_view_name} AS"
    stripped = view_sql.strip().rstrip(";")
    if stripped.startswith(header):
        body = stripped[len(header) :].strip()
        create_clause = header
    elif stripped.startswith(sqlite_header):
        body = stripped[len(sqlite_header) :].strip()
        create_clause = sqlite_header
    else:  # pragma: no cover - protects future view refactors
        raise RuntimeError(f"Unexpected view SQL header for {qualified_view_name}")
    return f"""
{create_clause}
SELECT
    base.*,
    {_ACTION_STATUS_SQL}
FROM (
{body}
) AS base;
"""


def upgrade() -> None:
    bind = op.get_bind()
    postgres_sql, sqlite_sql = _load_payment_audit_sql()
    if bind.dialect.name == "postgresql":
        op.execute("DROP VIEW IF EXISTS public.vw_payment_evidence_reconciliation;")
        op.execute(
            _wrap_view_sql(
                postgres_sql,
                qualified_view_name="public.vw_payment_evidence_reconciliation",
            )
        )
        op.execute(
            "COMMENT ON VIEW public.vw_payment_evidence_reconciliation IS "
            "'Audit-only payment evidence reconciliation view. reconciliation_result is a raw audit classification, not the Daily Sales Short Payments operator report.';"
        )
        op.execute(
            "COMMENT ON COLUMN public.vw_payment_evidence_reconciliation.reconciliation_result IS "
            "'Raw audit classification for reconciliation diagnostics; do not treat short/grouped short as the operator Short Payments report without operator_actionable_payment_status.';"
        )
        op.execute(
            "COMMENT ON COLUMN public.vw_payment_evidence_reconciliation.operator_actionable_payment_status IS "
            "'Derived audit marker: recovery workflow statuses are non_actionable_recovery_status; actionable_short_payment marks rows eligible for operator short-payment review.';"
        )
    else:
        op.execute("DROP VIEW IF EXISTS vw_payment_evidence_reconciliation;")
        op.execute(
            _wrap_view_sql(
                sqlite_sql, qualified_view_name="vw_payment_evidence_reconciliation"
            )
        )


def downgrade() -> None:
    # Forward-only documentation/compatibility projection update.
    return None
