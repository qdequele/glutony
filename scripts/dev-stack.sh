#!/usr/bin/env bash
# Bring the whole meili-ingest stack up locally and keep it running.
#
#   ./scripts/dev-stack.sh
#   open http://localhost:8080/ui/
#
# Starts Postgres and Meilisearch in Docker, a Temporal dev server, a local
# stand-in for Tinybird (so the usage dashboard has data), then the control
# plane, the gateway with the admin UI embedded, and two workers. Seeds a few
# jobs so the screens are not empty. Ctrl-C tears everything down.
#
# Requirements: docker, the temporal CLI, curl, jq, python3, cargo, and a UI
# export at ui/out (cd ui && NEXT_PUBLIC_BASE_PATH=/ui pnpm build).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GW_PORT=${GW_PORT:-8080}
CP_PORT=${CP_PORT:-9000}
PG_PORT=${PG_PORT:-55432}
MEILI_PORT=${MEILI_PORT:-7700}
TEMPORAL_PORT=${TEMPORAL_PORT:-7233}
TEMPORAL_UI_PORT=${TEMPORAL_UI_PORT:-8233}
TINYBIRD_PORT=${TINYBIRD_PORT:-58123}

WORK="${ROOT}/.dev-stack"
mkdir -p "$WORK"

export DATABASE_URL="postgres://postgres:dev@localhost:${PG_PORT}/postgres"
export TEMPORAL_URL="http://localhost:${TEMPORAL_PORT}"
export TEMPORAL_NAMESPACE=default
export CONTROL_PLANE_URL="http://localhost:${CP_PORT}"
export MEILI_URL="http://localhost:${MEILI_PORT}"
export MEILI_API_KEY=masterKey
export BLOB_STORE_URL="file://${WORK}/blobs"
export LOG_FORMAT=text
export RUST_LOG=${RUST_LOG:-info,meili_ingest=debug}
# Usage analytics against the local stand-in: workers append, the gateway reads.
export TINYBIRD_TOKEN=dev-tinybird-token
export TINYBIRD_READ_TOKEN=dev-tinybird-token
export TINYBIRD_BASE_URL="http://localhost:${TINYBIRD_PORT}"
export TINYBIRD_DATASOURCE=meili_ingest_usage

PIDS=()
cleanup() {
  set +e
  echo ""
  echo "--- stopping"
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  docker rm -f mi-dev-pg mi-dev-meili >/dev/null 2>&1
  echo "stopped. Data in ${WORK} is kept; delete it to start clean."
}
trap cleanup EXIT INT TERM

wait_for() {
  for _ in $(seq 1 90); do
    curl -fsS "$1" >/dev/null 2>&1 && { echo "  $2 ready"; return 0; }
    sleep 1
  done
  echo "  $2 did not come up: $1" >&2
  return 1
}

port_busy() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }
for port in "$GW_PORT" "$CP_PORT" "$MEILI_PORT" "$TEMPORAL_PORT" "$TINYBIRD_PORT"; do
  if port_busy "$port"; then
    echo "port $port is already in use; stop that process or set the matching *_PORT env var" >&2
    exit 1
  fi
done

echo "--- building (gateway with the admin UI embedded)"
UI_FEATURES=""
if [ -f ui/out/index.html ]; then
  UI_FEATURES="--features meili-ingest-gateway/ui"
else
  echo "  no ui/out export found — the gateway will be API-only."
  echo "  build it with: (cd ui && NEXT_PUBLIC_BASE_PATH=/ui pnpm build)"
fi
# shellcheck disable=SC2086
cargo build -q $UI_FEATURES --bin meili-ingest-gateway --bin meili-ingest-control-plane --bin meili-ingest-worker

echo "--- infrastructure"
docker rm -f mi-dev-pg mi-dev-meili >/dev/null 2>&1 || true
docker run -d --rm --name mi-dev-pg -p "${PG_PORT}:5432" -e POSTGRES_PASSWORD=dev postgres:17-alpine >/dev/null
docker run -d --rm --name mi-dev-meili -p "${MEILI_PORT}:7700" \
  -e MEILI_MASTER_KEY=masterKey -e MEILI_NO_ANALYTICS=true getmeili/meilisearch:v1.15 >/dev/null
temporal server start-dev --port "${TEMPORAL_PORT}" --ui-port "${TEMPORAL_UI_PORT}" \
  --db-filename "${WORK}/temporal.db" --log-level warn >"${WORK}/temporal.log" 2>&1 &
PIDS+=($!)
python3 scripts/fake_tinybird.py "${TINYBIRD_PORT}" >"${WORK}/tinybird.log" 2>&1 &
PIDS+=($!)

wait_for "${MEILI_URL}/health" meilisearch
for _ in $(seq 1 60); do docker exec mi-dev-pg pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
echo "  postgres ready"
for _ in $(seq 1 60); do temporal operator cluster health --address "localhost:${TEMPORAL_PORT}" >/dev/null 2>&1 && break; sleep 1; done
echo "  temporal ready"

echo "--- services"
BIND="0.0.0.0:${CP_PORT}" ./target/debug/meili-ingest-control-plane >"${WORK}/control-plane.log" 2>&1 &
PIDS+=($!)
wait_for "${CONTROL_PLANE_URL}/health" control-plane
BIND="0.0.0.0:${GW_PORT}" ./target/debug/meili-ingest-gateway >"${WORK}/gateway.log" 2>&1 &
PIDS+=($!)
wait_for "http://localhost:${GW_PORT}/health" gateway
TASK_QUEUE=workers-general ./target/debug/meili-ingest-worker >"${WORK}/worker-general.log" 2>&1 &
PIDS+=($!)
TASK_QUEUE=workers-gpu ./target/debug/meili-ingest-worker >"${WORK}/worker-gpu.log" 2>&1 &
PIDS+=($!)
sleep 3
echo "  workers polling"

GW="http://localhost:${GW_PORT}"

if [ "${SEED:-1}" = "1" ]; then
  echo "--- seeding a few jobs so the screens have data"
  python3 - "${WORK}/sample.pdf" <<'PYEOF'
import sys
pages = ["Meilisearch is a lightning fast search engine used by thousands of teams.",
         "meili-ingest turns documents, spreadsheets, decks and audio into search results."]
objs, page_ids = [], []
def add(o): objs.append(o); return len(objs)
font = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
parent = add("")
for text in pages:
    stream = f"BT /F1 12 Tf 50 750 Td ({text}) Tj ET".encode()
    content = add(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    page_ids.append(add(f"<< /Type /Page /Parent {parent} 0 R /MediaBox [0 0 612 792] /Contents {content} 0 R /Resources << /Font << /F1 {font} 0 R >> >> >>"))
objs[parent-1] = f"<< /Type /Pages /Kids [{' '.join(f'{p} 0 R' for p in page_ids)}] /Count {len(page_ids)} >>"
catalog = add(f"<< /Type /Catalog /Pages {parent} 0 R >>")
out, offsets = bytearray(b"%PDF-1.4\n"), []
for i, o in enumerate(objs, 1):
    offsets.append(len(out))
    out += f"{i} 0 obj\n".encode() + (o if isinstance(o, bytes) else o.encode()) + b"\nendobj\n"
xref = len(out)
out += f"xref\n0 {len(objs)+1}\n0000000000 65535 f \n".encode()
for off in offsets: out += f"{off:010d} 00000 n \n".encode()
out += f"trailer\n<< /Size {len(objs)+1} /Root {catalog} 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
open(sys.argv[1], "wb").write(out)
PYEOF
  printf 'sku,label,price\nA1,widget alpha,12\nB2,widget beta,30\nC3,widget gamma,7\n' > "${WORK}/rows.csv"
  curl -fsS -F "file=@${WORK}/sample.pdf" -F "index=documents" "${GW}/ingest" >/dev/null || true
  curl -fsS -F "file=@${WORK}/rows.csv" -F "index=datasets" "${GW}/ingest" >/dev/null || true
  curl -fsS -H 'Content-Type: application/json' \
    -d '{"documents":[{"id":"welcome","title":"Welcome","content":"inline json document"}],"index":"documents"}' \
    "${GW}/ingest" >/dev/null || true
  curl -fsS --data-binary "The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs." \
    -H 'Content-Type: text/plain' "${GW}/ingest?index=notes" >/dev/null || true
  sleep 4
  echo "  seeded $(curl -fsS "${GW}/jobs" | jq -r '.total // 0') jobs"
fi

cat <<BANNER

  meili-ingest is up.

    Admin UI      http://localhost:${GW_PORT}/ui/
    Gateway API   http://localhost:${GW_PORT}
    Temporal UI   http://localhost:${TEMPORAL_UI_PORT}
    Meilisearch   http://localhost:${MEILI_PORT}  (key: masterKey)

  Try an ingest:
    curl -F "file=@README.md" -F "index=docs" http://localhost:${GW_PORT}/ingest

  Logs are in ${WORK}/*.log
  Press Ctrl-C to stop everything.

BANNER

# Stay up until interrupted.
while true; do sleep 3600; done
