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
ENVOY_SECRET=e2e-envoy-secret
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
BIND="0.0.0.0:${GW_PORT}" ENVOY_TRUSTED_HEADER="${ENVOY_SECRET}" ./target/debug/meili-ingest-gateway >"${WORK}/gw.log" 2>&1 &
PIDS+=($!)
wait_for "http://localhost:${GW_PORT}/health" gateway
TASK_QUEUE=workers-general ./target/debug/meili-ingest-worker >"${WORK}/worker.log" 2>&1 &
PIDS+=($!)

# Mock OpenAI-compatible transcription endpoint so the audio pipeline can be tested
# without an API key. Returns a fixed transcript plus segments.
cat > "${WORK}/mock_transcribe.py" <<'PYEOF'
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

TRANSCRIPT = "Meilisearch ingests audio through the whisper transcriber plugin."
SEGMENTS = [
    {"id": 0, "start": 0.0, "end": 1.8, "text": "Meilisearch ingests audio"},
    {"id": 1, "start": 1.8, "end": 3.4, "text": "through the whisper transcriber plugin."},
]

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        if not self.path.endswith("/audio/transcriptions"):
            self.send_response(404); self.end_headers(); return
        body = json.dumps({
            "text": TRANSCRIPT, "language": "english", "duration": 3.4,
            "segments": SEGMENTS,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
PYEOF
python3 "${WORK}/mock_transcribe.py" "${TRANSCRIBE_PORT:-58111}" >"${WORK}/mock.log" 2>&1 &
PIDS+=($!)

TASK_QUEUE=workers-gpu \
  TRANSCRIBE_API_KEY=test-key \
  TRANSCRIBE_BASE_URL="http://localhost:${TRANSCRIBE_PORT:-58111}/v1" \
  ./target/debug/meili-ingest-worker >"${WORK}/worker-gpu.log" 2>&1 &
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

echo "--- ingest by URL reference (worker fetches it)"
python3 -m http.server "${FILE_PORT:-58099}" --directory "${WORK}" >"${WORK}/http.log" 2>&1 &
PIDS+=($!)
wait_for "http://localhost:${FILE_PORT:-58099}/sample.pdf" file-server
RESP4=$(curl -fsS -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://localhost:${FILE_PORT:-58099}/sample.pdf\",\"index\":\"e2e_url\"}" "${GW}/ingest")
echo "$RESP4" | jq .
JOB4=$(echo "$RESP4" | jq -r .job_id)
for _ in $(seq 1 90); do
  S=$(curl -fsS "${GW}/jobs/${JOB4}" | jq -r .status)
  [ "$S" = succeeded ] && break
  [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB4}" | jq .; tail -40 "${WORK}/worker.log"; exit 1; }
  sleep 1
done
[ "$S" = succeeded ] || { echo "url job state $S"; curl -fsS "${GW}/jobs/${JOB4}" | jq .; exit 1; }
sleep 1
UHITS=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_url/search" -H 'Content-Type: application/json' -d '{"q":"lightning"}' | jq '.estimatedTotalHits')
echo "e2e_url hits for 'lightning': $UHITS"; [ "$UHITS" -ge 1 ] || exit 1

echo "--- Envoy trust: headers without the shared secret are ignored"
# The gateway runs with ENVOY_TRUSTED_HEADER set, so X-Meili-* headers only count
# when X-Meili-Envoy-Secret matches. A spoofed host must be ignored and the request
# must fall back to MEILI_URL, landing the document in the real Meilisearch.
SPOOF=$(curl -fsS -H 'X-Meili-Host: http://evil.invalid:9' -H 'X-Meili-Api-Key: spoofed' \
  -H 'X-Meili-Index: e2e_evil' -H 'Content-Type: application/json' \
  -d '{"documents":[{"id":"s1","content":"spoofed tenant headers"}],"index":"e2e_spoof"}' "${GW}/ingest")
echo "$SPOOF" | jq -c .
[ "$(echo "$SPOOF" | jq -r .target_index)" = "e2e_spoof" ] || { echo "spoofed X-Meili-Index was honoured" >&2; exit 1; }
JOB_S=$(echo "$SPOOF" | jq -r .job_id)
for _ in $(seq 1 60); do S=$(curl -fsS "${GW}/jobs/${JOB_S}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && break; sleep 1; done
[ "$S" = succeeded ] || { echo "spoof-fallback job ended $S (expected succeeded via MEILI_URL fallback)" >&2; curl -fsS "${GW}/jobs/${JOB_S}" | jq .; exit 1; }
sleep 1
SH=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_spoof/search" -H 'Content-Type: application/json' -d '{"q":"spoofed"}' | jq '.estimatedTotalHits')
echo "e2e_spoof hits (must be in the REAL Meilisearch): $SH"; [ "$SH" -ge 1 ] || exit 1

echo "--- Envoy trust: headers with the shared secret are honoured"
TRUSTED=$(curl -fsS -H "X-Meili-Envoy-Secret: ${ENVOY_SECRET}" \
  -H "X-Meili-Host: ${MEILI_URL}" -H 'X-Meili-Api-Key: masterKey' \
  -H 'X-Meili-Index: e2e_tenant' -H 'X-Meili-Project-Id: acme' \
  -H 'Content-Type: application/json' \
  -d '{"documents":[{"id":"t1","content":"tenant routed by envoy headers"}]}' "${GW}/ingest")
echo "$TRUSTED" | jq -c .
[ "$(echo "$TRUSTED" | jq -r .target_index)" = "e2e_tenant" ] || { echo "trusted X-Meili-Index was ignored" >&2; exit 1; }
JOB_T=$(echo "$TRUSTED" | jq -r .job_id)
for _ in $(seq 1 60); do S=$(curl -fsS "${GW}/jobs/${JOB_T}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB_T}" | jq .; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "tenant job ended $S" >&2; exit 1; }
sleep 1
TH=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_tenant/search" -H 'Content-Type: application/json' -d '{"q":"tenant"}' | jq '.estimatedTotalHits')
echo "e2e_tenant hits: $TH"; [ "$TH" -ge 1 ] || exit 1

echo "--- user pipeline with a trigger wins over the builtin and sets the index"
cat > "${WORK}/trigger.yaml" <<'YAML'
uid: e2e-csv-datasets
name: "CSV rows into a fixed index"
trigger:
  content_types: [text/csv]
  index_pattern: e2e_from_trigger
steps:
  - id: parse
    plugin: csv_parser
  - id: index
    plugin: meili_indexer
YAML
curl -fsS -X POST -H 'Content-Type: application/x-yaml' --data-binary @"${WORK}/trigger.yaml" "${GW}/pipelines" | jq -r .uid
printf 'sku,label\nA1,widget alpha\nB2,widget beta\n' > "${WORK}/rows.csv"
# No ?index= and no X-Meili-Index: the pipeline trigger's index_pattern must decide.
RESP6=$(curl -fsS -F "file=@${WORK}/rows.csv" "${GW}/ingest")
echo "$RESP6" | jq -c .
[ "$(echo "$RESP6" | jq -r .pipeline_used)" = "e2e-csv-datasets" ] || { echo "user pipeline did not win over builtin.csv" >&2; exit 1; }
[ "$(echo "$RESP6" | jq -r .target_index)" = "e2e_from_trigger" ] || { echo "trigger index_pattern was not applied" >&2; exit 1; }
JOB6=$(echo "$RESP6" | jq -r .job_id)
for _ in $(seq 1 60); do S=$(curl -fsS "${GW}/jobs/${JOB6}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB6}" | jq .; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "csv job ended $S" >&2; exit 1; }
sleep 1
CH=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_from_trigger/stats" | jq .numberOfDocuments)
echo "e2e_from_trigger documents: $CH"; [ "$CH" -eq 2 ] || { echo "expected 2 csv rows" >&2; exit 1; }
curl -fsS -X DELETE "${GW}/pipelines/e2e-csv-datasets" -o /dev/null -w "delete pipeline → %{http_code}\n"

echo "--- pptx deck (pure-Rust OOXML extractor)"
python3 - "${WORK}/deck.pptx" <<'PYEOF'
import sys, zipfile
path = sys.argv[1]
NS = 'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"'
def slide(title, body):
    return f"""<?xml version="1.0"?><p:sld {NS}><p:cSld><p:spTree>
<p:sp><p:nvSpPr><p:nvPr><p:ph type="title"/></p:nvPr></p:nvSpPr>
<p:txBody><a:p><a:r><a:t>{title}</a:t></a:r></a:p></p:txBody></p:sp>
<p:sp><p:nvSpPr><p:nvPr/></p:nvSpPr>
<p:txBody><a:p><a:r><a:t>{body}</a:t></a:r></a:p></p:txBody></p:sp>
</p:spTree></p:cSld></p:sld>"""
slides = [
    ("Roadmap", "Vector search ships this quarter"),
    ("Ingestion", "meili-ingest handles pdf docx pptx and audio"),
]
with zipfile.ZipFile(path, "w") as z:
    z.writestr("[Content_Types].xml",
        '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="xml" ContentType="application/xml"/></Types>')
    for i, (t, b) in enumerate(slides, 1):
        z.writestr(f"ppt/slides/slide{i}.xml", slide(t, b))
PYEOF
RESP7=$(curl -fsS -F "file=@${WORK}/deck.pptx" -F "index=e2e_slides" "${GW}/ingest")
echo "$RESP7" | jq -c .
[ "$(echo "$RESP7" | jq -r .pipeline_used)" = "builtin.powerpoint" ] || { echo "pptx did not route to builtin.powerpoint" >&2; exit 1; }
JOB7=$(echo "$RESP7" | jq -r .job_id)
for _ in $(seq 1 60); do S=$(curl -fsS "${GW}/jobs/${JOB7}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB7}" | jq .; tail -30 "${WORK}/worker.log"; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "pptx job ended $S" >&2; exit 1; }
sleep 1
PH=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_slides/search" -H 'Content-Type: application/json' -d '{"q":"vector search"}' | jq '.estimatedTotalHits')
echo "e2e_slides hits for 'vector search': $PH"; [ "$PH" -ge 1 ] || exit 1

echo "--- audio transcription (builtin.audio on the workers-gpu pool)"
python3 - "${WORK}/clip.wav" <<'PYEOF'
import math, struct, sys, wave
path = sys.argv[1]
rate, secs, freq = 44100, 1.0, 440.0
with wave.open(path, "wb") as w:
    w.setnchannels(2); w.setsampwidth(2); w.setframerate(rate)
    frames = bytearray()
    for i in range(int(rate * secs)):
        v = int(20000 * math.sin(2 * math.pi * freq * i / rate))
        frames += struct.pack("<hh", v, v)
    w.writeframes(bytes(frames))
PYEOF
RESP8=$(curl -fsS -F "file=@${WORK}/clip.wav" -F "index=e2e_audio" "${GW}/ingest")
echo "$RESP8" | jq -c .
[ "$(echo "$RESP8" | jq -r .pipeline_used)" = "builtin.audio" ] || { echo "wav did not route to builtin.audio" >&2; exit 1; }
JOB8=$(echo "$RESP8" | jq -r .job_id)
for _ in $(seq 1 90); do S=$(curl -fsS "${GW}/jobs/${JOB8}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB8}" | jq .; tail -30 "${WORK}/worker-gpu.log"; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "audio job ended $S" >&2; tail -30 "${WORK}/worker-gpu.log"; exit 1; }
sleep 1
AH=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_audio/search" -H 'Content-Type: application/json' -d '{"q":"whisper transcriber"}' | jq '.estimatedTotalHits')
echo "e2e_audio hits for 'whisper transcriber': $AH"; [ "$AH" -ge 1 ] || exit 1

echo "--- video_audio_extractor decode path (pure Rust, no ffmpeg)"
cat > "${WORK}/media.yaml" <<'YAML'
uid: e2e-media
name: "Decode audio then transcribe"
steps:
  - id: extract_audio
    plugin: video_audio_extractor
    config: { sample_rate: 16000, mono: true }
  - id: transcribe
    plugin: whisper_transcriber
    config: { segment_documents: true }
  - id: index
    plugin: meili_indexer
YAML
curl -fsS -X POST -H 'Content-Type: application/x-yaml' --data-binary @"${WORK}/media.yaml" "${GW}/pipelines" | jq -r .uid
RESP9=$(curl -fsS -F "file=@${WORK}/clip.wav" "${GW}/ingest/pipeline/e2e-media?index=e2e_media")
echo "$RESP9" | jq -c .
JOB9=$(echo "$RESP9" | jq -r .job_id)
for _ in $(seq 1 90); do S=$(curl -fsS "${GW}/jobs/${JOB9}" | jq -r .status); [ "$S" = succeeded ] && break; [ "$S" = failed ] && { curl -fsS "${GW}/jobs/${JOB9}" | jq .; tail -40 "${WORK}/worker-gpu.log"; exit 1; }; sleep 1; done
[ "$S" = succeeded ] || { echo "media job ended $S" >&2; tail -40 "${WORK}/worker-gpu.log"; exit 1; }
sleep 1
MD=$(curl -fsS -H "Authorization: Bearer masterKey" "${MEILI_URL}/indexes/e2e_media/stats" | jq .numberOfDocuments)
echo "e2e_media documents (one per transcript segment): $MD"; [ "$MD" -ge 2 ] || { echo "expected segment documents" >&2; exit 1; }
curl -fsS -X DELETE "${GW}/pipelines/e2e-media" -o /dev/null -w "delete pipeline → %{http_code}\n"

echo "--- cancel a running job"
RESP5=$(curl -fsS -F "file=@${WORK}/sample.pdf" -F "index=e2e_cancel" "${GW}/ingest")
JOB5=$(echo "$RESP5" | jq -r .job_id)
curl -fsS -X POST "${GW}/jobs/${JOB5}/cancel" | jq .
for _ in $(seq 1 30); do
  S=$(curl -fsS "${GW}/jobs/${JOB5}" | jq -r .status)
  case "$S" in cancelled|succeeded) break;; esac
  sleep 1
done
echo "cancelled job final status: $S"
case "$S" in cancelled|succeeded) ;; *) echo "unexpected status $S" >&2; exit 1;; esac

echo "=== E2E PASSED ==="
