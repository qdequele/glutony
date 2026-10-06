#!/usr/bin/env python3
"""A stand-in for the Meilisearch Lab's event receiver, for scripts/e2e.sh --lab.

POST /internal/events  checks X-Lab-Signature (sha256=<hex HMAC-SHA256(secret, body)>),
                       stores each event once by id, answers {"accepted": [ids]}.
GET  /events           every stored event, as a JSON array.
"""

import hashlib
import hmac
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT, SECRET, OUT = int(sys.argv[1]), sys.argv[2].encode(), sys.argv[3]
EVENTS: dict[str, dict] = {}


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path != "/internal/events":
            return self._json(404, {"error": "not found"})
        expected = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(self.headers.get("X-Lab-Signature", ""), expected):
            return self._json(401, {"error": "bad signature"})
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
        self._json(404, {"error": "not found"})

    def log_message(self, *_):
        pass


HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
