"""Configuration scaffolding for the CRM downloader module.

The concrete settings (URLs, credentials, schedules, etc.) will be
implemented alongside the downloader workflows in a follow-up task.
This module currently exposes directory helpers so both downloaders can
share a consistent layout.
"""

from __future__ import annotations

from pathlib import Path


PKG_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PKG_ROOT.parent
DATA_DIR = PKG_ROOT / "data"
PROFILES_DIR = PKG_ROOT / "profiles"
SCRIPTS_DIR = PKG_ROOT / "scripts"

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROFILES_DIR.mkdir(parents=True, exist_ok=True)
SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)


def default_download_dir() -> Path:
    """Return the default directory for CRM download payloads."""

    return DATA_DIR


def default_profiles_dir() -> Path:
    """Return the default directory that stores Playwright profiles."""

    return PROFILES_DIR
