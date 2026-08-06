-- Preserve raw gateway observations in the existing tables.  These tables
-- hold server-side, MAC-based logical sessions and their source gateways.

CREATE TABLE device_broadcast_sessions (
    id BIGSERIAL PRIMARY KEY,
    device_mac VARCHAR(17) NOT NULL REFERENCES devices(device_mac),
    device_name VARCHAR(64),
    started_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    end_detected_at TIMESTAMPTZ,
    duration_s INTEGER,
    time_synced BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_device_broadcast_sessions_device_started
    ON device_broadcast_sessions (device_mac, started_at DESC);
CREATE INDEX ix_device_broadcast_sessions_open_last_seen
    ON device_broadcast_sessions (last_seen_at) WHERE ended_at IS NULL;

CREATE TABLE device_broadcast_observers (
    id BIGSERIAL PRIMARY KEY,
    global_session_id BIGINT NOT NULL REFERENCES device_broadcast_sessions(id) ON DELETE CASCADE,
    gateway_id VARCHAR(64) NOT NULL REFERENCES gateways(gateway_id),
    gateway_broadcast_id VARCHAR(48) NOT NULL,
    started_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    end_detected_at TIMESTAMPTZ,
    last_rssi INTEGER,
    time_synced BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_observer_gateway_broadcast UNIQUE (gateway_id, gateway_broadcast_id)
);
CREATE INDEX ix_device_broadcast_observers_global
    ON device_broadcast_observers (global_session_id);
