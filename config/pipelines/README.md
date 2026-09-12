# Example pipelines

Pipeline definitions are YAML (or JSON) documents matching `PipelineDefinition`
in `crates/plugin-sdk/src/types.rs`:

| Field | Description |
|---|---|
| `uid` | Unique id, `[a-zA-Z0-9._-]+`. `builtin.*` is reserved. |
| `name`, `description`, `version` | Display metadata. `version` is bumped automatically on update. |
| `trigger.content_types` | MIME types (exact or `type/*`) that auto-select this pipeline on `POST /ingest`. |
| `trigger.filename_pattern` | Optional glob on the filename (`contract_*.pdf`). |
| `trigger.index_pattern` | Optional target index, overrides `X-Meili-Index` / `?index=`. |
| `steps[]` | The DAG. `id`, `plugin`, optional `depends_on`, `fan_out` (`$.documents`, `$.many` or `$`), `config`, `timeout_secs`, `retry {max_attempts, backoff: exponential\|linear\|none, initial_interval_secs}`. |

Steps without `depends_on` run after the previous step (implicit sequential
mode). Cycles and unknown plugins are rejected with `422`.

## Register a pipeline

```bash
# YAML body
curl -X POST --data-binary @pdf-with-enrichment.yaml \
     -H 'Content-Type: application/x-yaml' \
     http://localhost:8080/pipelines

# JSON works too
curl -X POST -H 'Content-Type: application/json' \
     -d '{"uid":"csv-to-products","steps":[{"id":"parse","plugin":"csv_parser"},{"id":"index","plugin":"meili_indexer"}]}' \
     http://localhost:8080/pipelines
```

Behind Meilisearch Cloud the pipeline is scoped to the calling project
(`X-Meili-Project-Id`); self-hosted it is global.

## Use it

```bash
# Auto-routed: matches trigger (application/pdf + contract_*.pdf)
curl -F "file=@contract_2024.pdf" http://localhost:8080/ingest

# Explicit
curl -F "file=@keynote.mp4" "http://localhost:8080/ingest/pipeline/video-ingest?index=keynotes"

# Inspect / delete
curl http://localhost:8080/pipelines
curl http://localhost:8080/pipelines/my-pdf-with-enrichment
curl -X DELETE http://localhost:8080/pipelines/my-pdf-with-enrichment
```

## Files

| File | What it shows |
|---|---|
| `pdf-with-enrichment.yaml` | The SPEC §5 example: extract → chunk → fan-out LLM enrichment → index, with a filename trigger. |
| `video-ingest.yaml` | Multi-pool pipeline: GPU plugins (ffmpeg, Whisper) → chunker → LLM → indexer, with `index_pattern`. |
