-- When the Lab first answered 200 for a batch holding this row without listing it in
-- `accepted` (it skipped the event on purpose, spec v2 §3.4). NULL while the row only
-- saw whole-batch failures (unreachable, 401, 5xx, malformed answer). The sender drops
-- a skipped row 24 h after its first skip, and any undelivered row after 7 days
-- (crate::lab_sender). This supersedes the comment of 0004: undelivered rows can be
-- deleted. Additive only: older code ignores the column.
ALTER TABLE lab_events ADD COLUMN IF NOT EXISTS first_skipped_at TIMESTAMPTZ;
