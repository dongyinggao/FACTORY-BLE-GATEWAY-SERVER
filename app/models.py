from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import relationship

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


class DeviceBroadcastSession(Base):
    """One logical device broadcast, merged from one or more gateways."""

    __tablename__ = "device_broadcast_sessions"

    id = Column(Integer, primary_key=True)
    device_mac = Column(ForeignKey("devices.device_mac"), index=True, nullable=False)
    device_name = Column(String(64))
    started_at = Column(DateTime(timezone=True), index=True)
    last_seen_at = Column(DateTime(timezone=True), index=True)
    ended_at = Column(DateTime(timezone=True), index=True)
    end_detected_at = Column(DateTime(timezone=True))
    duration_s = Column(Integer)
    close_reason = Column(String(32))
    time_synced = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now, nullable=False, index=True)

    observers = relationship(
        "DeviceBroadcastObserver",
        back_populates="global_session",
        cascade="all, delete-orphan",
    )


class DeviceBroadcastObserver(Base):
    """A gateway's evidence for a DeviceBroadcastSession."""

    __tablename__ = "device_broadcast_observers"
    __table_args__ = (
        UniqueConstraint("gateway_id", "gateway_broadcast_id", name="uq_observer_gateway_broadcast"),
        UniqueConstraint("global_session_id", "gateway_id", name="uq_observer_global_gateway"),
    )

    id = Column(Integer, primary_key=True)
    global_session_id = Column(
        ForeignKey("device_broadcast_sessions.id"), index=True, nullable=False
    )
    gateway_id = Column(ForeignKey("gateways.gateway_id"), index=True, nullable=False)
    gateway_broadcast_id = Column(String(48), nullable=False)
    started_at = Column(DateTime(timezone=True), index=True)
    last_seen_at = Column(DateTime(timezone=True), index=True)
    ended_at = Column(DateTime(timezone=True))
    end_detected_at = Column(DateTime(timezone=True))
    last_rssi = Column(Integer)
    time_synced = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now, nullable=False)

    global_session = relationship("DeviceBroadcastSession", back_populates="observers")
