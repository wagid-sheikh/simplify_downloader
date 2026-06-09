"""Add rebuild row counts to OLI email."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0127_oli_rebuild_rows_email"
down_revision = "0126_oli_rebuild_notif"
branch_labels = None
depends_on = None

PIPELINE_CODE = "order_line_items_rebuild"
PROFILE_CODE = "default"
TEMPLATE_NAME = "default"

OLD_TEMPLATE_BODY = """
{% set payload = metrics_json.get('notification_payload', {}) %}
{% set zero_counts = payload.get('zero_snapshot_counts', {}) %}
Order Line Items Rebuild Run Summary
Run ID: {{ run_id }}
Environment: {{ run_env }}
Pipeline: {{ pipeline_name }}
Overall Status: {{ overall_status }}

Timing:
- Started: {{ payload.get('started_at') or started_at }}
- Finished: {{ payload.get('finished_at') or finished_at }}
- Duration: {{ payload.get('total_time_taken') or total_time_taken }}

Selection:
- Source Selection: {{ payload.get('source_selection') or 'unknown' }}
- Sources: {{ payload.get('sources', []) | join(', ') if payload.get('sources') else 'unknown' }}
- Stores: {{ payload.get('selected_stores', []) | join(', ') if payload.get('selected_stores') else 'all selected stores' }}
- Dry Run: {{ payload.get('dry_run') }}
- Resume: {{ payload.get('resume') }}
- Resume Run ID: {{ payload.get('resume_run_id') or 'none' }}

Windows:
- Expected: {{ payload.get('expected_window_count', 0) }}
- Completed: {{ payload.get('completed_window_count', 0) }}
- Missing: {{ payload.get('missing_window_count', 0) }}
- Skipped/Resumable: {{ payload.get('skipped_window_count', 0) }}

Zero Snapshot Warnings:
- Total Zero Snapshots: {{ zero_counts.get('zero_snapshot_count', 0) }}
- Suspicious: {{ zero_counts.get('suspicious_zero_snapshot_count', 0) }}
- Ambiguous: {{ zero_counts.get('ambiguous_zero_snapshot_count', 0) }}
- Source Fetch Failures: {{ zero_counts.get('source_fetch_failure_zero_snapshot_count', 0) }}
- Confirmed Source Empty: {{ zero_counts.get('confirmed_empty_snapshot_count', 0) }}

{% if payload.get('missing_windows') %}
Missing Windows:
{% for window in payload.get('missing_windows', []) %}
- {{ window.source }}:{{ window.store_code }} {{ window.window_start }}..{{ window.window_end }}
{% endfor %}
{% endif %}
{% if payload.get('skipped_windows') %}
Skipped/Resumable Windows:
{% for window in payload.get('skipped_windows', []) %}
- {{ window.source }}:{{ window.store_code }} {{ window.window_start }}..{{ window.window_end }}
{% endfor %}
{% endif %}

Outcome:
{% if overall_status == 'success' %}
- Success: all expected rebuild windows completed without warnings.
{% elif overall_status == 'failed' %}
- Failure: rebuild failed before all expected windows completed. Review warnings and run logs.
{% else %}
- Warning: rebuild completed with missing, skipped, or suspicious zero-snapshot windows.
{% endif %}

Warnings:
{% if payload.get('warnings') %}
{% for warning in payload.get('warnings', []) %}
- {{ warning }}
{% endfor %}
{% else %}
- None.
{% endif %}

Summary Text:
{{ summary_text or 'No additional summary text recorded.' }}
"""

NEW_TEMPLATE_BODY = OLD_TEMPLATE_BODY.replace(
    """Windows:\n- Expected: {{ payload.get('expected_window_count', 0) }}\n- Completed: {{ payload.get('completed_window_count', 0) }}\n- Missing: {{ payload.get('missing_window_count', 0) }}\n- Skipped/Resumable: {{ payload.get('skipped_window_count', 0) }}\n\nZero Snapshot Warnings:""",
    """Windows:\n- Expected: {{ payload.get('expected_window_count', 0) }}\n- Completed: {{ payload.get('completed_window_count', 0) }}\n- Missing: {{ payload.get('missing_window_count', 0) }}\n- Skipped/Resumable: {{ payload.get('skipped_window_count', 0) }}\n\n{% if payload.get('store_rows') %}\nPer Store Rows Rebuilt:\n{% for store in payload.get('store_rows', []) %}\n- {{ store.source }}:{{ store.store_code }}{% if store.cost_center %} ({{ store.cost_center }}){% endif %}: rows_rebuilt={{ store.rows_rebuilt or store.inserted_rows or 0 }} across {{ store.window_count or 0 }} window(s); deleted_rows={{ store.deleted_rows or 0 }}; inspected_orders={{ store.inspected_orders or 0 }}; complete_with_rows_orders={{ store.complete_with_rows_orders or 0 }}; complete_empty_orders={{ store.complete_empty_orders or 0 }}; skipped_incomplete_orders={{ store.skipped_incomplete_orders or 0 }}; orphan_rows={{ store.orphan_rows or 0 }}\n{% endfor %}\n{% endif %}\n{% if payload.get('completed_windows') %}\nPer Window Rows Rebuilt:\n{% for window in payload.get('completed_windows', []) %}\n- {{ window.source }}:{{ window.store_code }} {{ window.window_start }}..{{ window.window_end }}{% if window.cost_center %} ({{ window.cost_center }}){% endif %}: rows_rebuilt={{ window.rows_rebuilt if window.rows_rebuilt is defined else (window.inserted_rows or 0) }}; deleted_rows={{ window.deleted_rows or 0 }}; inspected_orders={{ window.inspected_orders or 0 }}; complete_with_rows_orders={{ window.complete_with_rows_orders or 0 }}; complete_empty_orders={{ window.complete_empty_orders or 0 }}; skipped_incomplete_orders={{ window.skipped_incomplete_orders or 0 }}; orphan_rows={{ window.orphan_rows or 0 }}; status={{ window.window_status or 'completed' }}\n{% endfor %}\n{% endif %}\nZero Snapshot Warnings:""",
)


def _tables() -> tuple[sa.Table, sa.Table, sa.Table]:
    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))
    profiles = sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
    )
    templates = sa.table(
        "email_templates",
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("body_template"),
    )
    return pipelines, profiles, templates


def _update_template(body_template: str) -> None:
    connection = op.get_bind()
    pipelines, profiles, templates = _tables()
    profile_ids = sa.select(profiles.c.id).where(
        profiles.c.pipeline_id == sa.select(pipelines.c.id)
        .where(pipelines.c.code == PIPELINE_CODE)
        .scalar_subquery(),
        profiles.c.code == PROFILE_CODE,
    )
    connection.execute(
        templates.update()
        .where(templates.c.profile_id.in_(profile_ids))
        .where(templates.c.name == TEMPLATE_NAME)
        .values(body_template=body_template)
    )


def upgrade() -> None:
    _update_template(NEW_TEMPLATE_BODY)


def downgrade() -> None:
    _update_template(OLD_TEMPLATE_BODY)
