-- Last observation is needed to distinguish a long active broadcast from a
-- missing end event. The gateway sends BROADCAST_ACTIVE at most once/minute.
ALTER TABLE broadcast_sessions
    ADD COLUMN last_seen_at TIMESTAMPTZ;

UPDATE broadcast_sessions
SET last_seen_at = COALESCE(ended_at, started_at)
WHERE last_seen_at IS NULL;

CREATE INDEX ix_broadcast_sessions_last_seen
    ON broadcast_sessions (last_seen_at DESC);
