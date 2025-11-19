from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Sequence

__all__ = [
    "main",
    "run_pipeline",
    "common",
    "dashboard_downloader",
    "crm_downloader",
    "alembic",
    "scripts",
    "docs",
    "tests",
]

_PACKAGE_ROOT = Path(__file__).resolve().parent
__path__ = [str(_PACKAGE_ROOT)]
if __spec__ is not None:
    __spec__.submodule_search_locations = __path__

_ALIAS_MODULES = {
    "common": "common",
    "dashboard_downloader": "dashboard_downloader",
    "crm_downloader": "crm_downloader",
    "alembic": "alembic",
    "scripts": "scripts",
    "docs": "docs",
    "tests": "tests",
}


def main(argv: Sequence[str] | None = None) -> int:
    """Compatibility wrapper for the legacy ``python -m simplify_downloader`` CLI."""

    from dashboard_downloader.cli import main as cli_main

    return cli_main(list(argv) if argv is not None else None)


def run_pipeline(*args: Any, **kwargs: Any) -> Any:
    """Shorthand for :func:`dashboard_downloader.pipeline.run_pipeline`."""

    from dashboard_downloader.pipeline import run_pipeline as _run_pipeline

    return _run_pipeline(*args, **kwargs)


def __getattr__(name: str) -> Any:
    if name == "main":
        return main
    if name == "run_pipeline":
        return run_pipeline
    if name in _ALIAS_MODULES:
        module = importlib.import_module(_ALIAS_MODULES[name])
        sys.modules[f"{__name__}.{name}"] = module
        return module
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


if __name__ == "__main__":
    raise SystemExit(main())
