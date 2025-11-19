"""Compatibility module for legacy ``python -m simplify_downloader`` entrypoints."""

from __future__ import annotations

from app import simplify_downloader as _impl

__all__ = getattr(_impl, "__all__", [])


def main(*args, **kwargs):  # type: ignore[override]
    return _impl.main(*args, **kwargs)


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_impl)))


if __name__ == "__main__":
    raise SystemExit(_impl.main())
