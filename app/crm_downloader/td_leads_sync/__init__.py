"""TD CRM leads sync package."""

__all__ = ["main"]


def __getattr__(name: str):
    if name == "main":
        from .main import main as td_leads_sync_main

        return td_leads_sync_main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
