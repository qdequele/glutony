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
-- Odd values cannot abort it: a field that is not a JSON number counts as 0 (a
-- `cost_complete` that is not a boolean as false), negatives as 0, fractions round up,
-- and values are capped like the builder's (units at 2^64 - 1, the cost at 2^63 - 1).
--
-- Idempotent: a converted row has `data.operation`, so a second run matches nothing.
-- Delivered rows are left alone. The control plane also runs this file after every
-- insert into the outbox (crate::lab_events), so events posted by workers not yet
-- upgraded are converted too; never edit it once released.

WITH old AS (
    SELECT id,
           body,
           body->'data' AS d,
           CASE WHEN jsonb_typeof(body->'data'->'cost_complete') = 'boolean'
                THEN (body->'data'->'cost_complete')::boolean
                ELSE false END AS complete,
           CASE WHEN jsonb_typeof(body->'data'->'duration_ms') = 'number'
                THEN greatest((body->'data'->'duration_ms')::numeric, 0)
                ELSE 0 END AS duration_ms,
           CASE WHEN jsonb_typeof(body->'data'->'cost_micro_usd') = 'number'
                THEN least(ceil(greatest((body->'data'->'cost_micro_usd')::numeric, 0)),
                           9223372036854775807)
                ELSE 0 END AS cost
    FROM lab_events
    WHERE delivered_at IS NULL
      AND body->>'type' = 'usage.recorded'
      AND jsonb_typeof(body->'data') = 'object'
      AND NOT (body->'data' ? 'operation')
      AND body->'data' ? 'cost_complete'
),
-- Every old unit as a non-negative whole number (0 when it is not a number).
units AS (
    SELECT old.id,
           coalesce(jsonb_object_agg(
               u.key,
               CASE WHEN jsonb_typeof(u.value) = 'number'
                    THEN least(ceil(greatest(u.value::numeric, 0)), 18446744073709551615)
                    ELSE 0 END
           ) FILTER (WHERE u.key IS NOT NULL), '{}'::jsonb) AS u
    FROM old
    LEFT JOIN LATERAL jsonb_each(
        CASE WHEN jsonb_typeof(old.d->'units') = 'object' THEN old.d->'units'
             ELSE '{}'::jsonb END
    ) AS u ON true
    GROUP BY old.id
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
                'documents', coalesce((units.u->>'documents_out')::numeric, 0),
                'bytes_in', coalesce((units.u->>'input_bytes')::numeric, 0),
                'step_seconds', least(ceil(old.duration_ms / 1000), 18446744073709551615),
                'llm_tokens_in', coalesce((units.u->>'llm_input_tokens')::numeric, 0),
                'llm_tokens_out', coalesce((units.u->>'llm_output_tokens')::numeric, 0),
                'audio_seconds', coalesce((units.u->>'audio_seconds')::numeric, 0),
                'ocr_pages', 0,
                'pages', coalesce((units.u->>'pages')::numeric, 0),
                'images', coalesce((units.u->>'images')::numeric, 0),
                'llm_requests', coalesce((units.u->>'llm_requests')::numeric, 0),
                'external_requests', coalesce((units.u->>'external_requests')::numeric, 0),
                'unpriced_provider_calls', CASE WHEN old.complete THEN 0 ELSE 1 END
            ),
            'provider_cost_micro_usd', old.cost,
            'description', format(
                'Job %s (%s, %s, %s documents%s)',
                old.d->>'job_id',
                old.d->>'pipeline_uid',
                old.d->>'status',
                coalesce((units.u->>'documents_out')::numeric, 0),
                CASE WHEN old.complete THEN '' ELSE ', provider cost incomplete' END
            ),
            'job_id', coalesce(old.d->>'job_id', '')
        )
    ),
    attempts = 0,
    next_attempt = now(),
    created_at = now()
FROM old
JOIN units ON units.id = old.id
WHERE e.id = old.id;
