from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Gateway(Base):
    __tablename__ = "gateways"

    gateway_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    location: Mapped[str | None] = mapped_column(String(160))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now)
    wifi_status: Mapped[str | None] = mapped_column(String(32))
    mqtt_status: Mapped[str | None] = mapped_column(String(32))
    sntp_status: Mapped[str | None] = mapped_column(String(32))
    sd_ready: Mapped[bool | None] = mapped_column(Boolean)
    latest_health: Mapped[dict | None] = mapped_column(JSON)


class Device(Base):
    __tablename__ = "devices"

    device_mac: Mapped[str] = mapped_column(String(17), primary_key=True)
    latest_name: Mapped[str | None] = mapped_column(String(64))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now)


class BroadcastEvent(Base):
    __tablename__ = "broadcast_events"
    __table_args__ = (UniqueConstraint("gateway_id", "event_id", name="uq_broadcast_gateway_event"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gateway_id: Mapped[str] = mapped_column(ForeignKey("gateways.gateway_id"), index=True)
    event_id: Mapped[str] = mapped_column(String(96))
    broadcast_id: Mapped[str] = mapped_column(String(48), index=True)
    device_mac: Mapped[str] = mapped_column(ForeignKey("devices.device_mac"), index=True)
    device_name: Mapped[str | None] = mapped_column(String(64))
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    recorded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    broadcast_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    broadcast_ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_s: Mapped[int | None] = mapped_column(Integer)
    observed_rssi: Mapped[int | None] = mapped_column(Integer)
    time_synced: Mapped[bool] = mapped_column(Boolean, default=False)
    event_uptime_s: Mapped[int | None] = mapped_column(Integer)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now, index=True)
    payload: Mapped[dict] = mapped_column(JSON)


class BroadcastSession(Base):
    __tablename__ = "broadcast_sessions"
    __table_args__ = (UniqueConstraint("gateway_id", "broadcast_id", name="uq_session_gateway_broadcast"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gateway_id: Mapped[str] = mapped_column(ForeignKey("gateways.gateway_id"), index=True)
    broadcast_id: Mapped[str] = mapped_column(String(48))
    device_mac: Mapped[str] = mapped_column(ForeignKey("devices.device_mac"), index=True)
    device_name: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_s: Mapped[int | None] = mapped_column(Integer)
    last_rssi: Mapped[int | None] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now)


class GatewayHealth(Base):
    __tablename__ = "gateway_health"
    __table_args__ = (UniqueConstraint("gateway_id", "event_id", name="uq_health_gateway_event"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gateway_id: Mapped[str] = mapped_column(ForeignKey("gateways.gateway_id"), index=True)
    event_id: Mapped[str] = mapped_column(String(96))
    uptime_s: Mapped[int | None] = mapped_column(Integer)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now, index=True)
    payload: Mapped[dict] = mapped_column(JSON)
