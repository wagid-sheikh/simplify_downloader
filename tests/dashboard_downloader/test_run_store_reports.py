import importlib
import sys
import types

import pytest


@pytest.fixture()
def run_store_reports_module(tmp_path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    fake_config_module = types.ModuleType("app.config")
    fake_config_module.config = types.SimpleNamespace(
        reports_root=str(reports_dir),
        database_url="sqlite://",
        td_base_url="https://example.com",
        td_login_url="https://example.com/login",
        td_home_url="https://example.com/home",
        tms_base="https://example.com",
        td_store_dashboard_path="/dash/{store_code}",
        td_storage_state_filename="storage_state.json",
        td_global_username="user",
        td_global_password="pass",
    )
    original_config = sys.modules.get("app.config")
    sys.modules["app.config"] = fake_config_module

    module = importlib.import_module("app.dashboard_downloader.run_store_reports")
    importlib.reload(module)

    yield module

    sys.modules.pop("app.dashboard_downloader.run_store_reports", None)
    if original_config is not None:
        sys.modules["app.config"] = original_config
    else:
        sys.modules.pop("app.config", None)


def test_resolve_template_file_accepts_directory(tmp_path, run_store_reports_module):
    template_dir = tmp_path / "templates"
    template_dir.mkdir()
    template_file = template_dir / run_store_reports_module.STORE_TEMPLATE_FILE_NAME
    template_file.write_text("<html></html>")

    resolved = run_store_reports_module._resolve_template_file(template_dir)

    assert resolved == template_file


def test_resolve_template_file_accepts_file(tmp_path, run_store_reports_module):
    template_file = tmp_path / "custom_template.html"
    template_file.write_text("<html></html>")

    resolved = run_store_reports_module._resolve_template_file(template_file)

    assert resolved == template_file


def test_resolve_template_file_missing(tmp_path, run_store_reports_module):
    missing_path = tmp_path / "does_not_exist.html"

    with pytest.raises(FileNotFoundError):
        run_store_reports_module._resolve_template_file(missing_path)
