#!/usr/bin/env python3
"""A stand-in for the Meilisearch Lab (platform contract v2), for scripts/e2e.sh --lab.

    fake_lab.py PORT INSTANCE_ID INSTANCE_SECRET OUT

POST /internal/events          checks X-Lab-Instance-Id, X-Lab-Timestamp (at most
                               300 s off) and X-Lab-Signature
                               (sha256=<hex HMAC-SHA256(secret, "<timestamp>.<body>")>),
                               stores each event once by id (appended to OUT),
                               answers {"accepted": [ids]}.
GET  /internal/instances/me    this deployment: a hosted glutony engine.
GET  /internal/accounts/{id}   every account is active with 100 credits.
GET  /events                   every stored event, as a JSON array.

The two /internal GETs require Authorization: Bearer <secret> and X-Lab-Instance-Id.
"""

import hashlib
import hmac
import json
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT, INSTANCE_ID, SECRET, OUT = (
    int(sys.argv[1]),
    sys.argv[2],
    sys.argv[3].encode(),
    sys.argv[4],
)
LAB_URL = f"http://127.0.0.1:{PORT}"
MAX_SKEW = 300
EVENTS: dict[str, dict] = {}
ACCOUNT = re.compile(r"^/internal/accounts/([0-9a-f-]{36})$")


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _instance_ok(self) -> bool:
        return hmac.compare_digest(self.headers.get("X-Lab-Instance-Id", ""), INSTANCE_ID)

    def _bearer_ok(self) -> bool:
        expected = "Bearer " + SECRET.decode()
        return self._instance_ok() and hmac.compare_digest(
            self.headers.get("Authorization", ""), expected
        )

    def _batch_error(self, body: bytes) -> str | None:
        if not self._instance_ok():
            return "unknown instance"
        ts = self.headers.get("X-Lab-Timestamp", "")
        if not ts.isdigit() or abs(time.time() - int(ts)) > MAX_SKEW:
            return "stale or missing timestamp"
        mac = hmac.new(SECRET, ts.encode() + b"." + body, hashlib.sha256)
        if not hmac.compare_digest(
            self.headers.get("X-Lab-Signature", ""), "sha256=" + mac.hexdigest()
        ):
            return "bad signature"
        return None

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path != "/internal/events":
            return self._json(404, {"error": "not found"})
        error = self._batch_error(body)
        if error:
            print(f"rejected a batch: {error}", file=sys.stderr, flush=True)
            return self._json(401, {"error": error})
        accepted = []
        for event in json.loads(body)["events"]:
            if event["id"] not in EVENTS:
                EVENTS[event["id"]] = event
                with open(OUT, "a") as f:
                    f.write(json.dumps(event) + "\n")
            accepted.append(event["id"])
        self._json(200, {"accepted": accepted})

    def do_GET(self):
        if self.path == "/events":
            return self._json(200, list(EVENTS.values()))
        if not self.path.startswith("/internal/"):
            return self._json(404, {"error": "not found"})
        if not self._bearer_ok():
            return self._json(401, {"error": "unauthorized"})
        if self.path == "/internal/instances/me":
            return self._json(
                200,
                {
                    "instance_id": INSTANCE_ID,
                    "kind": "hosted",
                    "product": "glutony",
                    "region": "local",
                    "lab_url": LAB_URL,
                },
            )
        m = ACCOUNT.match(self.path)
        if m:
            return self._json(
                200,
                {
                    "active": True,
                    "account_id": m.group(1),
                    "tier": "free",
                    "credits": {"balance": 100},
                    "cache_ttl": 30,
                },
            )
        self._json(404, {"error": "not found"})

    def log_message(self, *_):
        pass


ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
