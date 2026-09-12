#!/usr/bin/env bash
# End-to-end smoke test for meili-ingest on a developer machine.
#
# Starts: Temporal dev server (temporal CLI), Postgres + Meilisearch (Docker),
# then the control plane, gateway and a general worker from this workspace, ingests a
# PDF and a JSON document through the gateway, and checks that documents land in
# Meilisearch. Everything is torn down at the end.
#
# Requirements: docker, temporal CLI, curl, jq, python3, cargo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_PORT=${PG_PORT:-55432}
MEILI_PORT=${MEILI_PORT:-57700}
TEMPORAL_PORT=${TEMPORAL_PORT:-57233}
CP_PORT=${CP_PORT:-59000}
GW_PORT=${GW_PORT:-58080}
WORK="$(mktemp -d)"
export DATABASE_URL="postgres://postgres:dev@localhost:${PG_PORT}/postgres"
export TEMPORAL_URL="http://localhost:${TEMPORAL_PORT}"
export TEMPORAL_NAMESPACE=default
export CONTROL_PLANE_URL="http://localhost:${CP_PORT}"
export MEILI_URL="http://localhost:${MEILI_PORT}"
export MEILI_API_KEY=masterKey
export BLOB_STORE_URL="file://${WORK}/blobs"
export INLINE_MAX_BYTES=${INLINE_MAX_BYTES:-1048576}
export LOG_FORMAT=text
export RUST_LOG=${RUST_LOG:-info,meili_ingest=debug}

PIDS=()
cleanup() {
  set +e
  echo "--- cleaning up"
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  docker rm -f mi-e2e-pg mi-e2e-meili >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_for() { # url, name
  for _ in $(seq 1 60); do
    if curl -fsS "$1" >/dev/null 2>&1; then echo "$2 ready"; return 0; fi
    sleep 1
  done
  echo "$2 did not become ready: $1" >&2; return 1
}

echo "--- building"
cargo build -q --bin meili-ingest-gateway --bin meili-ingest-control-plane --bin meili-ingest-worker

echo "--- infra"
docker rm -f mi-e2e-pg mi-e2e-meili >/dev/null 2>&1 || true
docker run -d --rm --name mi-e2e-pg -p "${PG_PORT}:5432" -e POSTGRES_PASSWORD=dev postgres:17-alpine >/dev/null
docker run -d --rm --name mi-e2e-meili -p "${MEILI_PORT}:7700" -e MEILI_MASTER_KEY=masterKey -e MEILI_NO_ANALYTICS=true getmeili/meilisearch:v1.15 >/dev/null
temporal server start-dev --headless --port "${TEMPORAL_PORT}" --db-filename "${WORK}/temporal.db" --log-level warn >"${WORK}/temporal.log" 2>&1 &
PIDS+=($!)
wait_for "${MEILI_URL}/health" meilisearch
for _ in $(seq 1 60); do docker exec mi-e2e-pg pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
for _ in $(seq 1 60); do temporal operator cluster health --address "localhost:${TEMPORAL_PORT}" >/dev/null 2>&1 && break; sleep 1; done
echo "temporal ready"

echo "--- services"
BIND="0.0.0.0:${CP_PORT}" ./target/debug/meili-ingest-control-plane >"${WORK}/cp.log" 2>&1 &
PIDS+=($!)
wait_for "${CONTROL_PLANE_URL}/health" control-plane
BIND="0.0.0.0:${GW_PORT}" ./target/debug/meili-ingest-gateway >"${WORK}/gw.log" 2>&1 &
PIDS+=($!)
wait_for "http://localhost:${GW_PORT}/health" gateway
TASK_QUEUE=workers-general ./target/debug/meili-ingest-worker >"${WORK}/worker.log" 2>&1 &
PIDS+=($!)
sleep 3

GW="http://localhost:${GW_PORT}"
echo "--- pipelines & plugins"
curl -fsS "${GW}/pipelines" | jq -r '.[].uid' | sort | tr '\n' ' '; echo
curl -fsS "${GW}/plugins" | jq -r '.[].name' | sort | tr '\n' ' '; echo

echo "--- generating a PDF"
python3 - "${WORK}/sample.pdf" <<'PY'
import sys, zlib
path = sys.argv[1]
pages = ["Meilisearch is a lightning fast search engine. It powers the meili-ingest pipeline test.",
         "Second page: Temporal workflows orchestrate plugin activities across worker pools."]
objs = []
def add(o): objs.append(o); return len(objs)
font = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
page_ids = []
kids_placeholder = add("")  # pages object, filled later
for text in pages:
    stream = f"BT /F1 12 Tf 50 750 Td ({text}) Tj ET".encode()
    content = add(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    page = add(f"<< /Type /Page /Parent {kids_placeholder} 0 R /MediaBox [0 0 612 792] /Contents {content} 0 R /Resources << /Font << /F1 {font} 0 R >> >> >>")
    page_ids.append(page)
objs[kids_placeholder-1] = f"<< /Type /Pages /Kids [{' '.join(f'{p} 0 R' for p in page_ids)}] /Count {len(page_ids)} >>"
catalog = add(f"<< /Type /Catalog /Pages {kids_placeholder} 0 R >>")
out = bytearray(b"%PDF-1.4\n")
offsets = []
for i, o in enumerate(objs, 1):
    offsets.append(len(out))
    body = o if isinstance(o, bytes) else o.encode()
    out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
xref = len(out)
out += f"xref\n0 {len(objs)+1}\n0000000000 65535 f \n".encode()
for off in offsets: out += f"{off:010d} 00000 n \n".encode()
out += f"trailer\n<< /Size {len(objs)+1} /Root {catalog} 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
open(path, "wb").write(out)
PY

echo "--- ingest PDF (multipart, auto-routing)"
RESP=$(curl -fsS -F "file=@${WORK}/sample.pdf" -F "index=e2e_docs" "${GW}/ingest")
echo "$RESP" | jq .
JOB=$(echo "$RESP" | jq -r .job_id)
[ "$(echo "$RESP" | jq -r .pipeline_used)" = "builtin.pdf" ] || { echo "expected builtin.pdf" >&2; exit 1; }

echo "--- ingest inline JSON documents"
RESP2=$(curl -fsS -H 'Content-Type: application/json' -d '{"documents":[{"id":"j1","title":"Hello","content":"inline json document about search"}],"index":"e2e_json"}' "${GW}/ingest")
echo "$RESP2" | jq .
JOB2=$(echo "$RESP2" | jq -r .job_id)

echo "--- waiting for jobs"
for J in "$JOB" "$JOB2"; do
  for _ in $(seq 1 90); do
    S=$(curl -fsS "${GW}/jobs/${J}" | jq -r .status)
    case "$S" in
      succeeded) echo "job $J succeeded"; break;;
      failed|cancelled) echo "job $J $S"; curl -fsS "${GW}/jobs/${J}" | jq .; tail -50 "${WORK}/worker.log"; exit 1;;
    esac
    sleep 1
  done
  [ "$S" = succeeded ] || { echo "job $J timed out in state $S"; curl -fsS "${GW}/jobs/${J}" | jq .; tail -80 "${WORK}/worker.log"; exit 1; }
done
curl -fsS "${GW}/jobs/${JOB}" | jq .

echo "--- verifying Meilisearch"
sleep 1
HITS=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_docs/search" -H 'Content-Type: application/json' -d '{"q":"temporal"}' | jq '.estimatedTotalHits')
echo "e2e_docs hits for 'temporal': $HITS"
[ "$HITS" -ge 1 ] || { echo "no hits in e2e_docs" >&2; exit 1; }
HITS2=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_json/search" -H 'Content-Type: application/json' -d '{"q":"inline"}' | jq '.estimatedTotalHits')
echo "e2e_json hits for 'inline': $HITS2"
[ "$HITS2" -ge 1 ] || { echo "no hits in e2e_json" >&2; exit 1; }

echo "--- explicit pipeline + YAML pipeline CRUD"
cat > "${WORK}/p.yaml" <<'YAML'
uid: e2e-text-fixed
name: "Fixed chunking for text"
steps:
  - id: chunk
    plugin: chunker
    config: { strategy: fixed, chunk_size: 40, overlap: 5 }
  - id: index
    plugin: meili_indexer
YAML
curl -fsS -X POST -H 'Content-Type: application/x-yaml' --data-binary @"${WORK}/p.yaml" "${GW}/pipelines" | jq -r .uid
RESP3=$(curl -fsS --data-binary "The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs." -H 'Content-Type: text/plain' "${GW}/ingest/pipeline/e2e-text-fixed?index=e2e_text")
echo "$RESP3" | jq .
JOB3=$(echo "$RESP3" | jq -r .job_id)
for _ in $(seq 1 60); do S=$(curl -fsS "${GW}/jobs/${JOB3}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB3}" | jq .; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "job $JOB3 state $S"; exit 1; }
N=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_text/stats" | jq .numberOfDocuments)
echo "e2e_text chunks: $N"; [ "$N" -ge 2 ] || exit 1
curl -fsS -X DELETE "${GW}/pipelines/e2e-text-fixed" -o /dev/null -w "delete pipeline → %{http_code}\n"
curl -sS -X DELETE "${GW}/pipelines/builtin.pdf" -o /dev/null -w "delete builtin → %{http_code} (expect 403)\n"

echo "=== E2E PASSED ==="
