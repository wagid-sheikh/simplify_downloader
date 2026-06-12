"""Prevent overlapping enabled followup cap ranges."""

from __future__ import annotations

from alembic import op


revision = "0130_cfcc_no_overlap"
down_revision = "0129_cfl_source_identity"
branch_labels = None
depends_on = None

_GLOBAL_CONSTRAINT = "ex_cfcc_enabled_global_range"
_STORE_CONSTRAINT = "ex_cfcc_enabled_store_range"
_SQLITE_INSERT_TRIGGER = "trg_cfcc_no_overlap_insert"
_SQLITE_UPDATE_TRIGGER = "trg_cfcc_no_overlap_update"

_OVERLAP_EXISTS_SQL = """
EXISTS (
    SELECT 1
    FROM customer_followup_cap_config AS existing
    WHERE existing.enabled = 1
      AND existing.cap_config_id <> NEW.cap_config_id
      AND existing.lead_source_type = NEW.lead_source_type
      AND existing.work_section = NEW.work_section
      AND (
            (NEW.cost_center IS NULL AND existing.cost_center IS NULL)
            OR NEW.cost_center = existing.cost_center
          )
      AND NOT (
            (NEW.effective_until IS NOT NULL AND NEW.effective_until < existing.effective_from)
            OR (existing.effective_until IS NOT NULL AND existing.effective_until < NEW.effective_from)
          )
)
"""


def _upgrade_postgresql() -> None:
    # PostgreSQL daterange bounds are inclusive on both sides because effective_until is
    # a real covered business date. NULL effective_until is treated as unbounded.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM customer_followup_cap_config current_row
                JOIN customer_followup_cap_config other_row
                  ON current_row.cap_config_id < other_row.cap_config_id
                 AND current_row.enabled
                 AND other_row.enabled
                 AND current_row.cost_center IS NULL
                 AND other_row.cost_center IS NULL
                 AND current_row.lead_source_type = other_row.lead_source_type
                 AND current_row.work_section = other_row.work_section
                 AND daterange(
                        current_row.effective_from,
                        COALESCE(current_row.effective_until, 'infinity'::date),
                        '[]'
                     ) && daterange(
                        other_row.effective_from,
                        COALESCE(other_row.effective_until, 'infinity'::date),
                        '[]'
                     )
            ) THEN
                RAISE EXCEPTION 'customer_followup_cap_config has overlapping enabled global cap ranges';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM customer_followup_cap_config current_row
                JOIN customer_followup_cap_config other_row
                  ON current_row.cap_config_id < other_row.cap_config_id
                 AND current_row.enabled
                 AND other_row.enabled
                 AND current_row.cost_center IS NOT NULL
                 AND current_row.cost_center = other_row.cost_center
                 AND current_row.lead_source_type = other_row.lead_source_type
                 AND current_row.work_section = other_row.work_section
                 AND daterange(
                        current_row.effective_from,
                        COALESCE(current_row.effective_until, 'infinity'::date),
                        '[]'
                     ) && daterange(
                        other_row.effective_from,
                        COALESCE(other_row.effective_until, 'infinity'::date),
                        '[]'
                     )
            ) THEN
                RAISE EXCEPTION 'customer_followup_cap_config has overlapping enabled store cap ranges';
            END IF;
        END $$;
        """
    )
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute(
        f"""
        ALTER TABLE customer_followup_cap_config
        ADD CONSTRAINT {_GLOBAL_CONSTRAINT}
        EXCLUDE USING gist (
            lead_source_type WITH =,
            work_section WITH =,
            daterange(effective_from, COALESCE(effective_until, 'infinity'::date), '[]') WITH &&
        )
        WHERE (enabled AND cost_center IS NULL)
        """
    )
    op.execute(
        f"""
        ALTER TABLE customer_followup_cap_config
        ADD CONSTRAINT {_STORE_CONSTRAINT}
        EXCLUDE USING gist (
            cost_center WITH =,
            lead_source_type WITH =,
            work_section WITH =,
            daterange(effective_from, COALESCE(effective_until, 'infinity'::date), '[]') WITH &&
        )
        WHERE (enabled AND cost_center IS NOT NULL)
        """
    )


def _upgrade_sqlite() -> None:
    # SQLite is used by migration tests. These triggers intentionally mirror the
    # PostgreSQL exclusion predicates so tests exercise the same contract.
    op.execute(
        f"""
        CREATE TRIGGER {_SQLITE_INSERT_TRIGGER}
        BEFORE INSERT ON customer_followup_cap_config
        WHEN NEW.enabled = 1 AND {_OVERLAP_EXISTS_SQL}
        BEGIN
            SELECT RAISE(ABORT, 'overlapping enabled customer_followup_cap_config effective range');
        END
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_SQLITE_UPDATE_TRIGGER}
        BEFORE UPDATE OF cost_center, lead_source_type, work_section, enabled, effective_from, effective_until
        ON customer_followup_cap_config
        WHEN NEW.enabled = 1 AND {_OVERLAP_EXISTS_SQL}
        BEGIN
            SELECT RAISE(ABORT, 'overlapping enabled customer_followup_cap_config effective range');
        END
        """
    )


def upgrade() -> None:
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _upgrade_postgresql()
    elif dialect_name == "sqlite":
        _upgrade_sqlite()
    else:
        raise NotImplementedError(
            "customer_followup_cap_config overlap enforcement is implemented for PostgreSQL and SQLite tests only"
        )


def downgrade() -> None:
    # Forward-only migration.
    return None
