"""Compatibility wrapper for the dashboard monthly pipeline."""
from app.dashboard_downloader.pipelines.dashboard_monthly import *  # noqa: F401,F403

if __name__ == "__main__":
    from app.dashboard_downloader.pipelines.dashboard_monthly import run_pipeline

    run_pipeline()
