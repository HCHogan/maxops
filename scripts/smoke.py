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
        assert ctl("units.list", "--host", "fixture")["units"][0]["unit"] == "demo.service"
        assert ctl("deploy.status")["hosts"][0]["activated_at"] is None
        catalog = ctl("operations")
        assert len(catalog["operations"]) == 6

        client_env = os.environ.copy()
        client_env.update({"MAXOPS_URL": base, "MAXOPS_TOKEN_FILE": str(temp / "client")})
        http_catalog = json.loads(subprocess.check_output([
            "python3", str(root / "examples/http-client.py"), "operations"
        ], text=True, env=client_env))
        assert http_catalog["view"] == "summary"
        assert http_catalog["next_cursor"] is None
        assert [entry["name"] for entry in http_catalog["operations"]] == [entry["name"] for entry in catalog["operations"]]
        assert all("params_schema" not in entry and "response_schema" not in entry for entry in http_catalog["operations"])
        assert all("params_schema" in entry and "response_schema" in entry for entry in catalog["operations"])
        http_facts = json.loads(subprocess.check_output([
            "python3", str(root / "examples/http-client.py"), "call", "host.facts",
            json.dumps({"host": "fixture"})
        ], text=True, env=client_env))
        assert http_facts["facts"]["kernel"] == "synthetic"

        mcp = subprocess.Popen(
            [str(bin_dir / "maxops-mcp")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=client_env,
        )
        assert mcp.stdin is not None and mcp.stdout is not None

        def mcp_request(message):
            mcp.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
            mcp.stdin.flush()
            return json.loads(mcp.stdout.readline())

        initialized = mcp_request({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "smoke", "version": "1"}},
        })
        assert initialized["result"]["protocolVersion"] == "2025-06-18"
        mcp.stdin.write(json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized"
        }) + "\n")
        mcp.stdin.flush()
        mcp_tools = mcp_request({
            "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
        })["result"]["tools"]
        assert {tool["name"] for tool in mcp_tools} == {
            operation["name"] for operation in catalog["operations"]
        }
        assert all("inputSchema" in tool and "outputSchema" not in tool for tool in mcp_tools)
        mcp_facts = mcp_request({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "host.facts", "arguments": {"host": "fixture"}},
        })
        assert mcp_facts["result"]["structuredContent"]["facts"]["kernel"] == "synthetic"
        mcp.stdin.close()
        assert mcp.wait(timeout=5) == 0
        with urlopen(Request(base + "/v1/openapi.json", headers={"Authorization": f"Bearer {user_token}"})) as response:
            assert "paths" in json.load(response)
        try:
            urlopen(base + "/v1/operations")
            raise AssertionError("unauthenticated catalog was accepted")
        except HTTPError as error:
            assert error.code == 401
        print("PASS: real Hub, CLI, HTTP example and MCP share one scoped catalog and identity")
    finally:
        hub.terminate()
        try:
            hub.wait(timeout=5)
        except subprocess.TimeoutExpired:
            hub.kill()
            hub.wait()
        agent.shutdown()
        agent.server_close()
