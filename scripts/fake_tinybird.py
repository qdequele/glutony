#!/usr/bin/env python3
"""A stand-in for Tinybird, so the whole stack can be exercised locally.

Implements just enough of two APIs:

* ``POST /v0/events?name=<datasource>`` — the append path the workers use. Rows
  arrive as newline-delimited JSON and are kept in memory, deduplicated on
  ``event_id`` exactly as the real ``ReplacingMergeTree`` would, so a Temporal
  redelivery does not double-count here either.
* ``GET /v0/pipes/tenant_usage.json`` — the read path the gateway proxies for the
  dashboard. Aggregates the stored rows with the same partition rule as
  ``tinybird/pipes/usage_daily_billing.pipe``: job counters come only from
  ``kind='job'`` rows and work/cost counters only from ``kind='step'`` rows.

This exists for local development and the e2e script. Production points at a real
Tinybird workspace; see tinybird/README.md.
"""

import json
import sys
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

TOKEN = "dev-tinybird-token"
ROWS: dict[str, dict] = {}  # event_id -> row (last write wins, like ReplacingMergeTree)

JOB_COUNTERS = ("jobs", "jobs_succeeded", "jobs_failed")
STEP_COUNTERS = (
    "steps", "documents_out", "input_bytes", "duration_ms",
    "llm_input_tokens", "llm_output_tokens", "llm_requests",
    "audio_seconds", "pages", "images", "external_requests",
)


def aggregate(project_id: str, date_from: str, date_to: str) -> list[dict]:
    buckets: dict[tuple, dict] = defaultdict(
        lambda: {k: 0 for k in (*JOB_COUNTERS, *STEP_COUNTERS, "documents_indexed")}
    )
    for row in ROWS.values():
        if row.get("project_id", "") != project_id:
            continue
        day = str(row.get("ts", ""))[:10]
        if not day or day < date_from or day > date_to:
            continue
        kind = row.get("kind")
        key = (day, row.get("pipeline_uid", ""), row.get("plugin", "") if kind == "step" else "")
        bucket = buckets[key]
        if kind == "job":
            for k in JOB_COUNTERS:
                bucket[k] += row.get(k, 0) or 0
            bucket["jobs"] += 1 if not row.get("jobs") else 0
            # The job row carries the final step's output: the corpus that landed.
            bucket["documents_indexed"] += row.get("documents_out", 0) or 0
        else:
            for k in STEP_COUNTERS:
                bucket[k] += row.get(k, 0) or 0
            bucket["steps"] += 1 if not row.get("steps") else 0

    out = []
    for (day, pipeline_uid, plugin), metrics in sorted(buckets.items()):
        out.append({"day": day, "pipeline_uid": pipeline_uid, "plugin": plugin, **metrics})
    return out


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        return self.headers.get("Authorization") == f"Bearer {TOKEN}"

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if "/v0/events" not in self.path:
            self._json(404, {"error": "not found"})
            return
        if not self._authorized():
            self._json(403, {"error": "forbidden"})
            return
        accepted = 0
        for line in body.decode().splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                self._json(200, {"successful_rows": accepted, "quarantined_rows": 1})
                return
            ROWS[row.get("event_id", f"anon-{len(ROWS)}")] = row
            accepted += 1
        self._json(200, {"successful_rows": accepted, "quarantined_rows": 0})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/v0/pipes/tenant_usage"):
            self._json(404, {"error": "not found"})
            return
        if not self._authorized():
            self._json(403, {"error": "forbidden"})
            return
        q = parse_qs(parsed.query)
        data = aggregate(
            q.get("project_id", [""])[0],
            q.get("date_from", ["0000-01-01"])[0],
            q.get("date_to", ["9999-12-31"])[0],
        )
        self._json(200, {"meta": [], "data": data, "rows": len(data)})

    def log_message(self, *args) -> None:  # quiet
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 58123
    print(f"fake tinybird listening on {port}", flush=True)
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
