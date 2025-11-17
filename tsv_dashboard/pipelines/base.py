from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from dashboard_downloader.db_tables import pipeline_run_summaries
from simplify_downloader.common.db import session_scope
from simplify_downloader.config import config


Status = str


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PipelinePhaseTracker:
    pipeline_name: str
    env: str
    run_id: str
    started_at: datetime = field(default_factory=utcnow)
    report_date: date | None = None
    phases: Dict[str, Dict[Status, int]] = field(
        default_factory=lambda: {}
    )
    metrics: Dict[str, Any] = field(default_factory=dict)
    summary_lines: list[str] = field(default_factory=list)
    overall: Status = "ok"

    def mark_phase(self, phase: str, status: Status) -> None:
        counters = self.phases.setdefault(phase, {"ok": 0, "warning": 0, "error": 0})
        normalized = status if status in counters else "ok"
        counters[normalized] += 1
        if normalized == "error":
            self.overall = "error"
        elif normalized == "warning" and self.overall != "error":
            self.overall = "warning"

    def add_summary(self, line: str) -> None:
        self.summary_lines.append(line)

    def set_report_date(self, report_date: date) -> None:
        self.report_date = report_date

    def build_summary_text(self, finished_at: datetime) -> str:
        duration = finished_at - self.started_at
        total_seconds = max(0, int(duration.total_seconds()))
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        lines = [
            f"Pipeline: {self.pipeline_name}",
            f"Env: {self.env}",
            f"Run ID: {self.run_id}",
            f"Report Date: {self.report_date.isoformat() if self.report_date else 'unknown'}",
            f"Started At: {self.started_at.isoformat()} UTC",
            f"Finished At: {finished_at.isoformat()} UTC",
            f"Total Time: {hh:02d}:{mm:02d}:{ss:02d}",
            f"Overall Status: {self.overall}",
            "",
            "Phases:",
        ]
        for phase, counters in self.phases.items():
            phase_status = "error" if counters.get("error") else "warning" if counters.get("warning") else "ok"
            lines.append(f"- {phase}: {phase_status.upper()}")
        if self.summary_lines:
            lines.append("")
            lines.append("Details:")
            lines.extend(f"- {line}" for line in self.summary_lines)
        return "\n".join(lines)

    def build_record(self, finished_at: datetime) -> Dict[str, Any]:
        duration = finished_at - self.started_at
        total_seconds = max(0, int(duration.total_seconds()))
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "run_env": self.env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": f"{hh:02d}:{mm:02d}:{ss:02d}",
            "report_date": self.report_date,
            "overall_status": self.overall,
            "summary_text": self.build_summary_text(finished_at),
            "phases_json": self.phases,
            "metrics_json": self.metrics,
        }


async def check_existing_run(
    database_url: str, pipeline_name: str, report_date: date
) -> Mapping[str, Any] | None:
    async with session_scope(database_url) as session:
        stmt = (
            pipeline_run_summaries.select()
            .where(pipeline_run_summaries.c.pipeline_name == pipeline_name)
            .where(pipeline_run_summaries.c.report_date == report_date)
            .order_by(pipeline_run_summaries.c.id.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.mappings().first()


async def persist_summary_record(
    database_url: str, record: Mapping[str, Any]
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(pipeline_run_summaries.insert().values(**record))
        await session.commit()


async def update_summary_record(
    database_url: str, run_id: str, record: Mapping[str, Any]
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            pipeline_run_summaries.update()
            .where(pipeline_run_summaries.c.run_id == run_id)
            .values(**record)
        )
        await session.commit()


def resolve_run_env(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    return config.run_env or config.environment


def ensure_asyncio_run(coro: Iterable[Any] | Any) -> Any:
    if asyncio.iscoroutine(coro):
        return asyncio.run(coro)
    return coro
