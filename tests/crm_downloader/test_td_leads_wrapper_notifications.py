from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.crm_downloader.td_leads_sync import wrapper_notifications as notifications


def _event(**overrides):
    values = {
        "wrapper_timestamp": "2026-06-01T00:00:00Z",
        "hostname": "td-host",
        "local_lock_path": "/srv/simplify/tmp/cron_run_td_leads_sync.lock",
        "owner_pid": "123",
        "owner_pgid": "123",
        "owner_age_seconds": "301",
        "recovery_action": "terminated_watchdog_child_process_group",
        "status": "watchdog_timeout",
    }
    values.update(overrides)
    return notifications.build_wrapper_event(**values)


@pytest.mark.asyncio
async def test_watchdog_timeout_generates_initial_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted = []
    delivered = []
    monkeypatch.setattr(
        notifications, "_load_previous_events", lambda _url: _async_result([])
    )

    async def fake_persist(_url, event, *, disposition):
        persisted.append((event, disposition))
        return "tdlw-watchdog"

    async def fake_send(pipeline_code, run_id):
        delivered.append((pipeline_code, run_id))
        return {"emails_sent": 1, "emails_planned": 1, "errors": []}

    monkeypatch.setattr(notifications, "_persist_event", fake_persist)
    monkeypatch.setattr(notifications, "send_notifications_for_run", fake_send)

    result = await notifications.process_wrapper_event(_event())

    assert result["notification_disposition"] == "initial"
    assert result["emails_sent"] == 1
    assert persisted[0][0].status == "watchdog_timeout"
    assert delivered == [(notifications.PIPELINE_CODE, "tdlw-watchdog")]


@pytest.mark.asyncio
async def test_active_owner_suppression_is_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _event(
        status="skipped_due_to_active_same_pipeline_owner",
        recovery_action="suppressed_overlapping_invocation",
    )
    previous = [{"metrics_json": event.as_metrics()}]
    monkeypatch.setattr(
        notifications, "_load_previous_events", lambda _url: _async_result(previous)
    )
    monkeypatch.setattr(
        notifications,
        "_persist_event",
        lambda *_args, **_kwargs: _async_result("tdlw-suppressed"),
    )

    async def unexpected_send(*_args, **_kwargs):
        raise AssertionError("deduplicated suppression must not send an email")

    monkeypatch.setattr(notifications, "send_notifications_for_run", unexpected_send)

    result = await notifications.process_wrapper_event(event)

    assert result["notification_requested"] is False
    assert result["notification_disposition"] == "deduplicated_active_incident"
    assert result["emails_sent"] == 0


@pytest.mark.asyncio
async def test_recovery_event_generates_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _event(
        status=notifications.RECOVERY_STATUS,
        recovery_action="fresh_td_leads_run_completed_successfully",
    )
    previous_alert = _event(
        status="stale_owner_terminated",
        recovery_action="terminated_stale_owner_process_group",
    )
    monkeypatch.setattr(
        notifications,
        "_load_previous_events",
        lambda _url: _async_result([{"metrics_json": previous_alert.as_metrics()}]),
    )
    monkeypatch.setattr(
        notifications,
        "_persist_event",
        lambda *_args, **_kwargs: _async_result("tdlw-recovery"),
    )
    monkeypatch.setattr(
        notifications,
        "send_notifications_for_run",
        lambda pipeline_code, run_id: _async_result(
            {"emails_sent": 1, "emails_planned": 1, "errors": []}
        ),
    )

    result = await notifications.process_wrapper_event(event)

    assert result["notification_requested"] is True
    assert result["notification_disposition"] == "recovery"
    assert result["emails_sent"] == 1


def test_wrapper_event_redacts_sensitive_text() -> None:
    event = _event(
        hostname="td-host token=crm-token customer@example.com +91 98765 43210",
        local_lock_path="/tmp/password=hunter2/customer@example.com.lock",
        recovery_action="credential=secret-value mobile=9876543210",
    )
    rendered = notifications._event_summary(event)

    assert "crm-token" not in rendered
    assert "hunter2" not in rendered
    assert "secret-value" not in rendered
    assert "customer@example.com" not in rendered
    assert "98765" not in rendered
    assert "<redacted>" in rendered


@pytest.mark.asyncio
async def test_missing_notification_profile_is_reported_without_losing_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        notifications, "_load_previous_events", lambda _url: _async_result([])
    )
    monkeypatch.setattr(
        notifications,
        "_persist_event",
        lambda *_args, **_kwargs: _async_result("tdlw-no-profile"),
    )
    monkeypatch.setattr(
        notifications,
        "send_notifications_for_run",
        lambda pipeline_code, run_id: _async_result(
            {
                "emails_sent": 0,
                "emails_planned": 0,
                "errors": [
                    f"no active notification profiles found for pipeline {pipeline_code}"
                ],
            }
        ),
    )

    result = await notifications.process_wrapper_event(_event())

    assert result["run_id"] == "tdlw-no-profile"
    assert result["emails_sent"] == 0
    assert result["errors"] == [
        "no active notification profiles found for pipeline td_leads_wrapper_ops"
    ]


async def _async_result(value):
    return value


@pytest.mark.asyncio
async def test_smtp_failure_propagates_to_bounded_shell_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(notifications, "_load_previous_events", lambda _url: _async_result([]))
    monkeypatch.setattr(notifications, "_persist_event", lambda *_args, **_kwargs: _async_result("tdlw-smtp-failure"))

    async def _raise_smtp_failure(*_args, **_kwargs):
        raise RuntimeError("simulated SMTP failure")

    monkeypatch.setattr(notifications, "send_notifications_for_run", _raise_smtp_failure)

    with pytest.raises(RuntimeError, match="simulated SMTP failure"):
        await notifications.process_wrapper_event(_event())
