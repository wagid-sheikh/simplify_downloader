from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import sqlalchemy as sa


def _load_migration_module() -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0126_oli_rebuild_notif.py"
    spec = importlib.util.spec_from_file_location("v0126_oli_rebuild_notif", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


class _Result:
    def __init__(self, value: Any = None, rows: list[dict[str, Any]] | None = None) -> None:
        self._value = value
        self._rows = rows or []

    def scalar_one(self) -> Any:
        if self._value is None:
            raise AssertionError("Expected scalar_one value to be present")
        return self._value

    def scalar(self) -> Any:
        return self._value

    def mappings(self) -> list[dict[str, Any]]:
        return self._rows


class _RecordingConnection:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {
            "pipelines": [],
            "notification_profiles": [],
            "email_templates": [],
            "notification_recipients": [],
        }
        self._next_id = 1

    def execute(self, statement: Any, *_args: Any, **_kwargs: Any) -> _Result:
        table_name = getattr(getattr(statement, "table", None), "name", None)
        if table_name == "pipelines":
            payload = self._statement_values(statement)
            payload["id"] = self._next_id
            self._next_id += 1
            self.rows["pipelines"].append(payload)
            return _Result(payload["id"])
        if table_name == "notification_profiles":
            payload = self._statement_values(statement)
            payload["id"] = self._next_id
            self._next_id += 1
            self.rows["notification_profiles"].append(payload)
            return _Result(payload["id"])
        if table_name == "email_templates":
            payload = self._statement_values(statement)
            payload["id"] = self._next_id
            self._next_id += 1
            self.rows["email_templates"].append(payload)
            return _Result()
        if table_name == "notification_recipients":
            payload = self._statement_values(statement)
            payload["id"] = self._next_id
            self._next_id += 1
            self.rows["notification_recipients"].append(payload)
            return _Result()
        if isinstance(statement, sa.sql.Select):
            return _Result(None)
        raise AssertionError(f"Unexpected statement: {statement!r}")

    @staticmethod
    def _statement_values(statement: Any) -> dict[str, Any]:
        values = getattr(statement, "_values", {})
        return {key: getattr(value, "value", value) for key, value in values.items()}


def test_oli_rebuild_notif_upgrade_seeds_pipeline_profile_template_and_recipients(
    monkeypatch,
) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_bind=lambda: connection))

    migration.upgrade()

    assert connection.rows["pipelines"] == [
        {
            "id": 1,
            "code": "order_line_items_rebuild",
            "description": "Order Line Items Rebuild Pipeline",
        }
    ]
    assert connection.rows["notification_profiles"] == [
        {
            "id": 2,
            "pipeline_id": 1,
            "code": "default",
            "description": "Order line items rebuild run summary",
            "env": "any",
            "scope": "run",
            "attach_mode": "none",
            "is_active": True,
        }
    ]
    assert len(connection.rows["email_templates"]) == 1
    template = connection.rows["email_templates"][0]
    assert template["profile_id"] == 2
    assert template["name"] == "default"
    assert "Order Line Items Rebuild" in template["subject_template"]
    assert "notification_payload" in template["body_template"]

    recipients = connection.rows["notification_recipients"]
    assert {row["env"] for row in recipients} == {"dev", "prod", "local", "any"}
    assert all(row["profile_id"] == 2 for row in recipients)
    assert all(row["email_address"] == "wagid.sheikh@gmail.com" for row in recipients)
    assert all(row["send_as"] == "to" for row in recipients)
    assert all(row["is_active"] is True for row in recipients)
