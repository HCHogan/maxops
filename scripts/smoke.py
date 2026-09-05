#!/usr/bin/env python3
"""Exercise real hub and CLI binaries against a synthetic loopback-only agent."""
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import os
import secrets
import socket
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

root = Path(__file__).resolve().parents[1]
bin_dir = Path(os.environ.get("MAXOPS_BIN_DIR", root / "target/debug"))
agent_token = secrets.token_hex(32)


class Agent(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.headers.get("Authorization") != f"Bearer {agent_token}":
            self.send_error(401)
            return
        if self.path != "/v1/snapshot":
            self.send_error(404)
            return
        body = json.dumps({
            "host": "fixture", "observed_at": datetime.now(timezone.utc).isoformat(),
            "facts": {"kernel": "synthetic", "uptime_seconds": 123.0,
                      "system_closure": "/nix/store/synthetic-system"},
            "units": [{"unit": "demo.service", "description": "synthetic failed service",
                       "load_state": "loaded", "active_state": "failed", "sub_state": "failed"}],
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


with tempfile.TemporaryDirectory(prefix="maxops-smoke-") as directory:
    temp = Path(directory)
    user_token = secrets.token_hex(32)
    for name, token in [("agent", agent_token), ("client", user_token)]:
        (temp / name).write_text(token)
        (temp / name).chmod(0o600)
    agent = ThreadingHTTPServer(("127.0.0.1", 0), Agent)
    threading.Thread(target=agent.serve_forever, daemon=True).start()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    config = {
        "listen": f"127.0.0.1:{port}",
        "hosts": [{"name": "fixture", "agent_url": f"http://127.0.0.1:{agent.server_port}",
                   "agent_token_file": str(temp / "agent"), "readable_units": ["demo.service"]}],
        "clients": [{"name": "smoke", "token_file": str(temp / "client"), "hosts": ["fixture"],
                     "capabilities": ["fleet:read", "host:read", "units:read"]}],
    }
    config_path = temp / "hub.json"
    config_path.write_text(json.dumps(config))
    hub = subprocess.Popen([str(bin_dir / "maxops-hub"), "--config", str(config_path)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            if hub.poll() is not None:
                raise RuntimeError("hub exited before becoming ready")
            try:
                with urlopen(base + "/healthz", timeout=1) as response:
                    assert response.status == 200
                break
            except URLError:
                time.sleep(0.05)
        else:
            raise RuntimeError("hub readiness timeout")

        def ctl(*args):
            return json.loads(subprocess.check_output([
                str(bin_dir / "maxopsctl"), "--url", base,
                "--token-file", str(temp / "client"), *args], text=True))

        assert ctl("fleet.overview")["hosts"][0]["agent"]["failed_units"] == 1
        assert ctl("host.facts", "--host", "fixture")["facts"]["kernel"] == "synthetic"
        assert ctl("units.failed")["hosts"][0]["units"][0]["unit"] == "demo.service"
        assert len(ctl("operations")["operations"]) == 4
        with urlopen(Request(base + "/v1/openapi.json", headers={"Authorization": f"Bearer {user_token}"})) as response:
            assert "paths" in json.load(response)
        try:
            urlopen(base + "/v1/operations")
            raise AssertionError("unauthenticated catalog was accepted")
        except HTTPError as error:
            assert error.code == 401
        print("PASS: real hub + CLI round trips, scoped catalog, OpenAPI, unauthenticated rejection (synthetic agent)")
    finally:
        hub.terminate()
        try:
            hub.wait(timeout=5)
        except subprocess.TimeoutExpired:
            hub.kill()
            hub.wait()
        agent.shutdown()
        agent.server_close()
