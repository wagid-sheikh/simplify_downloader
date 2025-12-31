"""No-op shim to bridge the leads assignment template revisions."""

from __future__ import annotations


revision = "0023_leads_assignment_templates"
down_revision = "0023_lead_assignment_templates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Maintain continuity between the singular and plural revisions."""
    pass


def downgrade() -> None:
    """Revert to the singular revision without any schema changes."""
    pass
