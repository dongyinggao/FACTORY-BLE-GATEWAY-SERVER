CREATE TABLE gateways (
    gateway_id VARCHAR(64) PRIMARY KEY,
    location VARCHAR(160),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    wifi_status VARCHAR(32), mqtt_status VARCHAR(32), sntp_status VARCHAR(32),
    sd_ready BOOLEAN, latest_health JSONB
);

CREATE TABLE devices (
    device_mac VARCHAR(17) PRIMARY KEY, latest_name VARCHAR(64),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE broadcast_events (
    id BIGSERIAL PRIMARY KEY,
    gateway_id VARCHAR(64) NOT NULL REFERENCES gateways(gateway_id),
    event_id VARCHAR(96) NOT NULL, broadcast_id VARCHAR(48) NOT NULL,
    device_mac VARCHAR(17) NOT NULL REFERENCES devices(device_mac),
    device_name VARCHAR(64), event_type VARCHAR(32) NOT NULL,
    recorded_at TIMESTAMPTZ, broadcast_started_at TIMESTAMPTZ,
    broadcast_ended_at TIMESTAMPTZ, end_detected_at TIMESTAMPTZ,
    duration_s INTEGER, observed_rssi INTEGER, time_synced BOOLEAN NOT NULL DEFAULT FALSE,
    event_uptime_s INTEGER, received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), payload JSONB NOT NULL,
    CONSTRAINT uq_broadcast_gateway_event UNIQUE (gateway_id, event_id)
);
CREATE INDEX ix_broadcast_events_gateway_received ON broadcast_events (gateway_id, received_at DESC);
CREATE INDEX ix_broadcast_events_device_received ON broadcast_events (device_mac, received_at DESC);

CREATE TABLE broadcast_sessions (
    id BIGSERIAL PRIMARY KEY,
    gateway_id VARCHAR(64) NOT NULL REFERENCES gateways(gateway_id),
    broadcast_id VARCHAR(48) NOT NULL, device_mac VARCHAR(17) NOT NULL REFERENCES devices(device_mac),
    device_name VARCHAR(64), started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
    end_detected_at TIMESTAMPTZ, duration_s INTEGER, last_rssi INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_session_gateway_broadcast UNIQUE (gateway_id, broadcast_id)
);
CREATE INDEX ix_broadcast_sessions_device_updated ON broadcast_sessions (device_mac, updated_at DESC);

CREATE TABLE gateway_health (
    id BIGSERIAL PRIMARY KEY,
    gateway_id VARCHAR(64) NOT NULL REFERENCES gateways(gateway_id), event_id VARCHAR(96) NOT NULL,
    uptime_s INTEGER, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), payload JSONB NOT NULL,
    CONSTRAINT uq_health_gateway_event UNIQUE (gateway_id, event_id)
);
CREATE INDEX ix_gateway_health_gateway_recorded ON gateway_health (gateway_id, recorded_at DESC);
