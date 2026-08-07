ALTER TABLE device_broadcast_sessions
    ADD COLUMN close_reason VARCHAR(32);

-- Mark pre-existing derived records with the strongest conclusion their
-- observer evidence supports. This migration is safe immediately after a
-- backfill and does not alter raw events or local gateway sessions.
UPDATE device_broadcast_sessions AS global_session
SET close_reason = CASE
    WHEN EXISTS (
        SELECT 1
        FROM device_broadcast_observers AS observer
        WHERE observer.global_session_id = global_session.id
          AND observer.ended_at IS NULL
    ) THEN 'STALE_TIMEOUT'
    ELSE 'ALL_OBSERVERS_ENDED'
END
WHERE global_session.ended_at IS NOT NULL;
