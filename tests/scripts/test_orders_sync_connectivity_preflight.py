from __future__ import annotations

import http.server
import os
import subprocess
import threading
from pathlib import Path


class _FailingHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:  # noqa: N802 - stdlib callback name.
        self.send_response(500)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 - stdlib callback name.
        self.send_response(500)
        self.end_headers()
        self.wfile.write(b"not ready")

    def log_message(self, format: str, *args: object) -> None:
        return


def test_tcp_success_with_app_layer_failure_is_not_clean_success() -> None:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FailingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_port

    try:
        result = subprocess.run(
            ["bash", "scripts/orders_sync_connectivity_preflight.sh", "127.0.0.1"],
            cwd=Path.cwd(),
            env={
                **os.environ,
                "ORDERS_SYNC_CONNECTIVITY_PREFLIGHT_PORT": str(port),
                "ORDERS_SYNC_APP_LAYER_PREFLIGHT": "1",
                "ORDERS_SYNC_APP_LAYER_PREFLIGHT_SCHEME": "http",
                "ORDERS_SYNC_APP_LAYER_PREFLIGHT_METHOD": "HEAD",
                "ORDERS_SYNC_APP_LAYER_PREFLIGHT_EXPECTED_CLASSES": "2xx,3xx,4xx",
            },
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    combined_output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "tcp_connectivity_preflight_dns_ok" in combined_output
    assert "tcp_connectivity_preflight_tcp_ok" in combined_output
    assert "app_layer_preflight_http_failed" in combined_output
    assert "classification=app_layer_failed" in combined_output
    assert "tcp_connectivity_preflight_succeeded" not in combined_output
