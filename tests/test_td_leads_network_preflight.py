from __future__ import annotations

import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.crm_downloader.td_leads_sync import network_preflight


class _FakeSocket:
    def __init__(self, *, fail: OSError | None = None) -> None:
        self.fail = fail
        self.timeout = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def connect(self, sockaddr) -> None:
        if self.fail is not None:
            raise self.fail


def test_td_leads_preflight_classifies_crm_dns_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_getaddrinfo(host, port, type=0):
        raise socket.gaierror("no such host")

    monkeypatch.setattr(network_preflight.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(
        network_preflight,
        "probe_smtp_endpoint",
        lambda *, timeout_seconds: network_preflight.EndpointProbe(
            name="smtp_notifications",
            host="smtp.gmail.com",
            port=587,
            ok=True,
            dns_ok=True,
            tcp_ok=True,
            failure_class=None,
            elapsed_ms=1.0,
        ),
    )

    exit_code, payload = network_preflight.run_preflight(timeout_seconds=1)

    assert exit_code == network_preflight.EXIT_CRM_UNREACHABLE
    assert payload["classification"] == "crm_connectivity_failed"
    assert payload["browser_launch_allowed"] is False
    assert payload["crm"]["failure_class"] == "dns_resolution_failure"


def test_td_leads_preflight_classifies_crm_tcp_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        network_preflight.socket,
        "getaddrinfo",
        lambda host, port, type=0: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.1", port))
        ],
    )
    monkeypatch.setattr(
        network_preflight.socket,
        "socket",
        lambda family, socktype, proto: _FakeSocket(
            fail=ConnectionRefusedError("refused")
        ),
    )
    monkeypatch.setattr(
        network_preflight,
        "probe_smtp_endpoint",
        lambda *, timeout_seconds: network_preflight.EndpointProbe(
            name="smtp_notifications",
            host="smtp.gmail.com",
            port=587,
            ok=True,
            dns_ok=True,
            tcp_ok=True,
            failure_class=None,
            elapsed_ms=1.0,
        ),
    )

    exit_code, payload = network_preflight.run_preflight(timeout_seconds=1)

    assert exit_code == network_preflight.EXIT_CRM_UNREACHABLE
    assert payload["classification"] == "crm_connectivity_failed"
    assert payload["crm"]["dns_ok"] is True
    assert payload["crm"]["tcp_ok"] is False
    assert payload["crm"]["failure_class"] == "tcp_connection_failure"


def test_td_leads_preflight_pass_through_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        network_preflight,
        "probe_dns_tcp_endpoint",
        lambda **kwargs: network_preflight.EndpointProbe(
            name="td_crm",
            host="subs.quickdrycleaning.com",
            port=443,
            ok=True,
            dns_ok=True,
            tcp_ok=True,
            failure_class=None,
            elapsed_ms=1.0,
        ),
    )
    monkeypatch.setattr(
        network_preflight,
        "probe_smtp_endpoint",
        lambda *, timeout_seconds: network_preflight.EndpointProbe(
            name="smtp_notifications",
            host="smtp.gmail.com",
            port=587,
            ok=True,
            dns_ok=True,
            tcp_ok=True,
            failure_class=None,
            elapsed_ms=1.0,
        ),
    )

    exit_code, payload = network_preflight.run_preflight(timeout_seconds=1)

    assert exit_code == network_preflight.EXIT_OK
    assert payload["classification"] == "all_endpoints_ok"
    assert payload["browser_launch_allowed"] is True
    assert payload["notification_delivery_degraded"] is False


def test_td_leads_preflight_uses_smtp_probe_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_probe_smtp_tcp_connectivity(*, timeout_seconds=None, host=None, port=None):
        captured.update(
            {"timeout_seconds": timeout_seconds, "host": host, "port": port}
        )
        return {"ok": True, "host": host, "port": port, "elapsed_ms": 2.0}

    monkeypatch.setattr(
        network_preflight,
        "probe_smtp_tcp_connectivity",
        fake_probe_smtp_tcp_connectivity,
    )

    probe = network_preflight.probe_smtp_endpoint(timeout_seconds=3)

    assert probe.ok is True
    assert captured == {"timeout_seconds": 3, "host": "smtp.gmail.com", "port": 587}
