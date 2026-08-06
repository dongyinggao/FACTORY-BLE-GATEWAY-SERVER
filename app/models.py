from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint

from app.database import Base


class Gateway(Base):
    __tablename__ = "gateways"

    gateway_id = Column(String(64), primary_key=True)
    location = Column(String(160))
    first_seen_at = Column(DateTime(timezone=True), default=datetime.now)
    last_seen_at = Column(DateTime(timezone=True), default=datetime.now)
    wifi_status = Column(String(32))
    mqtt_status = Column(String(32))
    sntp_status = Column(String(32))
    sd_ready = Column(Boolean)
    latest_health = Column(JSON)


class Device(Base):
    __tablename__ = "devices"

    device_mac = Column(String(17), primary_key=True)
    latest_name = Column(String(64))
    first_seen_at = Column(DateTime(timezone=True), default=datetime.now)
    last_seen_at = Column(DateTime(timezone=True), default=datetime.now)


class BroadcastEvent(Base):
    __tablename__ = "broadcast_events"
    __table_args__ = (UniqueConstraint("gateway_id", "event_id", name="uq_broadcast_gateway_event"),)

    id = Column(Integer, primary_key=True)
    gateway_id = Column(ForeignKey("gateways.gateway_id"), index=True)
    event_id = Column(String(96))
    broadcast_id = Column(String(48), index=True)
    device_mac = Column(ForeignKey("devices.device_mac"), index=True)
    device_name = Column(String(64))
    event_type = Column(String(32), index=True)
    recorded_at = Column(DateTime(timezone=True), index=True)
    broadcast_started_at = Column(DateTime(timezone=True))
    broadcast_ended_at = Column(DateTime(timezone=True))
    end_detected_at = Column(DateTime(timezone=True))
    duration_s = Column(Integer)
    observed_rssi = Column(Integer)
    time_synced = Column(Boolean, default=False)
    event_uptime_s = Column(Integer)
    received_at = Column(DateTime(timezone=True), default=datetime.now, index=True)
    payload = Column(JSON)


class BroadcastSession(Base):
    __tablename__ = "broadcast_sessions"
    __table_args__ = (UniqueConstraint("gateway_id", "broadcast_id", name="uq_session_gateway_broadcast"),)

    id = Column(Integer, primary_key=True)
    gateway_id = Column(ForeignKey("gateways.gateway_id"), index=True)
    broadcast_id = Column(String(48))
    device_mac = Column(ForeignKey("devices.device_mac"), index=True)
    device_name = Column(String(64))
    started_at = Column(DateTime(timezone=True), index=True)
    ended_at = Column(DateTime(timezone=True))
    end_detected_at = Column(DateTime(timezone=True))
    duration_s = Column(Integer)
    last_rssi = Column(Integer)
    updated_at = Column(DateTime(timezone=True), default=datetime.now)


class GatewayHealth(Base):
    __tablename__ = "gateway_health"
    __table_args__ = (UniqueConstraint("gateway_id", "event_id", name="uq_health_gateway_event"),)

    id = Column(Integer, primary_key=True)
    gateway_id = Column(ForeignKey("gateways.gateway_id"), index=True)
    event_id = Column(String(96))
    uptime_s = Column(Integer)
    recorded_at = Column(DateTime(timezone=True), default=datetime.now, index=True)
    payload = Column(JSON)
