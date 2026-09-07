#!/usr/bin/env python3
"""Minimal maxops client using only the Python standard library."""

import argparse
import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

MAX_BODY = 2 * 1024 * 1024
TERMINAL = {"succeeded", "failed", "outcome_unknown", "cancelled", "timed_out"}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Maxops:
    def __init__(self, url: str, token_file: pathlib.Path) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("MAXOPS_URL must be an HTTP(S) origin")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("MAXOPS_URL must not contain credentials, query, or fragment")
        self.url = url.rstrip("/")
        self.token = token_file.read_text(encoding="ascii").rstrip("\r\n")
        if not 32 <= len(self.token) <= 512 or not all(33 <= ord(char) <= 126 for char in self.token):
            raise ValueError("token must contain 32..512 printable ASCII characters")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method: str, path: str, body=None, idempotency_key=None):
        encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        if encoded is not None and len(encoded) > MAX_BODY:
            raise ValueError("request exceeds 2 MiB")
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        request = urllib.request.Request(
            f"{self.url}{path}", data=encoded, headers=headers, method=method
        )
        try:
            with self.opener.open(request, timeout=15) as response:
                raw = response.read(MAX_BODY + 1)
        except urllib.error.HTTPError as error:
            # Only forward the bounded machine contract, never an arbitrary
            # proxy error page or a remote human-readable body.
            try:
                public = json.loads(error.read(4097))
                code, retry = public.get("code", ""), public.get("retry", "")
                if code not in {"unsupported_operation", "idempotency_conflict", "revision_conflict", "stale_baseline", "cursor_invalid", "workflow_conflict", "unauthenticated", "forbidden", "not_found", "state_conflict", "cursor_expired", "busy", "invalid_request", "unavailable"}:
                    code = "unknown"
                if retry not in {"refresh_catalog", "never", "refresh", "replan", "restart_listing", "observe", "backoff", "observe_before_retry"}:
                    retry = "unspecified"
            except (ValueError, AttributeError):
                code, retry = "unknown", "unspecified"
            raise RuntimeError(f"maxops HTTP {error.code} code={code} retry={retry}") from None
        if len(raw) > MAX_BODY:
            raise RuntimeError("maxops response exceeds 2 MiB")
        return json.loads(raw)

    def operations(self):
        return self.request("GET", "/v1/operations?view=summary")

    def execute(self, operation: str, params: dict, idempotency_key=None):
        return self.request(
            "POST",
            "/v1/execute",
            {"op": operation, "params": params},
            idempotency_key,
        )

    def wait(self, job_id: str):
        revision = None
        while True:
            response = self.execute("jobs.wait", {
                "job_id": job_id, "after_revision": revision, "timeout_seconds": 10,
            })
            job = response["job"]
            revision = job["handle"]["revision"]
            if job["handle"]["state"] in TERMINAL:
                return job


def json_object(value: str) -> dict:
    if value.startswith("@"):
        value = pathlib.Path(value[1:]).read_text(encoding="utf-8")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("params must be a JSON object")
    return parsed


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--url", default=os.environ.get("MAXOPS_URL", "http://127.0.0.1:9721"))
    result.add_argument(
        "--token-file",
        type=pathlib.Path,
        default=os.environ.get("MAXOPS_TOKEN_FILE"),
        required="MAXOPS_TOKEN_FILE" not in os.environ,
    )
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("operations")
    call = commands.add_parser("call")
    call.add_argument("operation")
    call.add_argument("params", type=json_object, help="JSON object or @path")
    call.add_argument("--idempotency-key")
    call.add_argument("--wait", action="store_true")
    wait = commands.add_parser("wait")
    wait.add_argument("job_id")
    return result


def main() -> int:
    args = parser().parse_args()
    client = Maxops(args.url, args.token_file)
    if args.command == "operations":
        response = client.operations()
    elif args.command == "wait":
        response = client.wait(args.job_id)
    else:
        response = client.execute(args.operation, args.params, args.idempotency_key)
        if args.wait:
            response = client.wait(response["job_id"])
    json.dump(response, sys.stdout, indent=2)
    sys.stdout.write("\n")
    state = response.get("handle", {}).get("state")
    return 0 if state in {None, "succeeded"} else 10


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(2) from None
