-- Convert pre-v2 Lab events still waiting in the outbox to the v2 `usage.recorded`
-- shape (platform contract v2, spec §4.2), so they are billed instead of rejected.
--
-- Before the v2 contract the worker wrote `usage.recorded` rows whose `data` was
-- {job_id, pipeline_uid, status, duration_ms, cost_micro_usd, cost_complete, units:
-- {documents_out, input_bytes, pages, images, audio_seconds (a float),
-- llm_input_tokens, llm_output_tokens, llm_requests, external_requests}}. A v2 Lab
-- accepts the batch, then rejects such an event at processing ("invalid operation"),
-- so it is never billed. Each undelivered one becomes what the current builder
-- (crates/usage/src/lab.rs, lab_event_for_job) emits:
--
-- * `step_seconds` is ceil(duration_ms / 1000): the old rows only kept the job's wall
--   time, not the sum of its step times the builder uses now.
-- * `ocr_pages` is 0: the old rows did not tell OCR steps apart.
-- * `unpriced_provider_calls` is 0 when the cost was complete, else 1: the old rows
--   only know that at least one call could not be priced.
-- * `provider_cost_micro_usd` is the old `cost_micro_usd`, already the priced sum.
--
-- The converted rows get a fresh 24 h delivery window (attempts, next_attempt and
-- created_at reset), or the sender's 24 h drop would discard them after one attempt.
-- They have no job lifecycle event (`job.completed` / `job.failed`): those are only
-- analytics, and the old worker never wrote them.
--
-- Idempotent: a converted row has `data.operation`, so a second run matches nothing.
-- Delivered rows are left alone.

WITH old AS (
    SELECT id,
           body,
           body->'data' AS d,
           body->'data'->'units' AS u,
           coalesce((body->'data'->>'cost_complete')::boolean, false) AS complete,
           coalesce((body->'data'->'units'->>'documents_out')::numeric, 0) AS documents
    FROM lab_events
    WHERE delivered_at IS NULL
      AND body->>'type' = 'usage.recorded'
      AND jsonb_typeof(body->'data') = 'object'
      AND NOT (body->'data' ? 'operation')
      AND body->'data' ? 'cost_complete'
)
UPDATE lab_events AS e
SET body = jsonb_build_object(
        'id', old.body->'id',
        'type', old.body->'type',
        'occurred_at', old.body->'occurred_at',
        'account_id', old.body->'account_id',
        'api_key_id', coalesce(old.body->'api_key_id', 'null'::jsonb),
        'product', old.body->'product',
        'data', jsonb_build_object(
            'operation', 'ingest',
            'units', jsonb_build_object(
                'documents', old.documents,
                'bytes_in', coalesce((old.u->>'input_bytes')::numeric, 0),
                'step_seconds', ceil(greatest(coalesce((old.d->>'duration_ms')::numeric, 0), 0) / 1000),
                'llm_tokens_in', coalesce((old.u->>'llm_input_tokens')::numeric, 0),
                'llm_tokens_out', coalesce((old.u->>'llm_output_tokens')::numeric, 0),
                'audio_seconds', ceil(greatest(coalesce((old.u->>'audio_seconds')::numeric, 0), 0)),
                'ocr_pages', 0,
                'pages', coalesce((old.u->>'pages')::numeric, 0),
                'images', coalesce((old.u->>'images')::numeric, 0),
                'llm_requests', coalesce((old.u->>'llm_requests')::numeric, 0),
                'external_requests', coalesce((old.u->>'external_requests')::numeric, 0),
                'unpriced_provider_calls', CASE WHEN old.complete THEN 0 ELSE 1 END
            ),
            'provider_cost_micro_usd', coalesce((old.d->>'cost_micro_usd')::numeric, 0),
            'description', format(
                'Job %s (%s, %s, %s documents%s)',
                old.d->>'job_id',
                old.d->>'pipeline_uid',
                old.d->>'status',
                old.documents,
                CASE WHEN old.complete THEN '' ELSE ', provider cost incomplete' END
            ),
            'job_id', old.d->>'job_id'
        )
    ),
    attempts = 0,
    next_attempt = now(),
    created_at = now()
FROM old
WHERE e.id = old.id;
