"""Compatibility wrapper for the dashboard weekly pipeline."""
from app.dashboard_downloader.pipelines.dashboard_weekly import *  # noqa: F401,F403

if __name__ == "__main__":
    from app.dashboard_downloader.pipelines.dashboard_weekly import run_pipeline

    run_pipeline()
