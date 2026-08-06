import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import BroadcastEvent, BroadcastSession, Device, Gateway, GatewayHealth

LOGGER = logging.getLogger(__name__)


def parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        # ESP-IDF strftime emits an RFC-822 offset (+0800); Python 3.10's
        # fromisoformat requires the ISO-8601 spelling (+08:00).
        normalized = value
        if len(value) >= 5 and value[-5] in "+-" and value[-2] != ":":
            normalized = f"{value[:-2]}:{value[-2:]}"
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError:
        LOGGER.warning("Ignoring invalid timestamp: %r", value)
        return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _upsert_gateway(session: Session, payload: dict[str, Any]) -> Gateway:
    gateway_id = payload["gateway_id"]
    gateway = session.get(Gateway, gateway_id)
    if gateway is None:
        gateway = Gateway(gateway_id=gateway_id, first_seen_at=_now())
        session.add(gateway)
    gateway.location = payload.get("gateway_location") or gateway.location
    gateway.last_seen_at = _now()
    return gateway


def _upsert_device(session: Session, payload: dict[str, Any]) -> Device:
    device_mac = payload["device_mac"]
    device = session.get(Device, device_mac)
    if device is None:
        device = Device(device_mac=device_mac, first_seen_at=_now())
        session.add(device)
    device.latest_name = payload.get("device_name") or device.latest_name
    device.last_seen_at = _now()
    return device


def ingest_broadcast(session: Session, payload: dict[str, Any]) -> bool:
    required = ("gateway_id", "event_id", "broadcast_id", "device_mac", "event")
    if any(not payload.get(key) for key in required):
        raise ValueError(f"broadcast is missing required field: {required}")

    gateway_id = payload["gateway_id"]
    event_id = payload["event_id"]
    duplicate = session.scalar(
        select(BroadcastEvent.id).where(
            BroadcastEvent.gateway_id == gateway_id,
            BroadcastEvent.event_id == event_id,
        )
    )
    if duplicate is not None:
        return False

    _upsert_gateway(session, payload)
    _upsert_device(session, payload)
    # BroadcastEvent has foreign keys to both rows. Flush parents explicitly:
    # SQLAlchemy 1.4 used by the offline UAT runtime cannot infer ordering here
    # because the lightweight ingest model deliberately has no ORM relations.
    session.flush()
    event = BroadcastEvent(
        gateway_id=gateway_id,
        event_id=event_id,
        broadcast_id=payload["broadcast_id"],
        device_mac=payload["device_mac"],
        device_name=payload.get("device_name"),
        event_type=payload["event"],
        recorded_at=parse_timestamp(payload.get("recorded_at")),
        broadcast_started_at=parse_timestamp(payload.get("broadcast_started_at")),
        broadcast_ended_at=parse_timestamp(payload.get("broadcast_ended_at")),
        end_detected_at=parse_timestamp(payload.get("end_detected_at")),
        duration_s=payload.get("broadcast_duration_s"),
        observed_rssi=payload.get("observed_rssi"),
        time_synced=bool(payload.get("time_synced", False)),
        event_uptime_s=payload.get("event_uptime_s"),
        received_at=_now(),
        payload=payload,
    )
    session.add(event)

    session_row = session.scalar(
        select(BroadcastSession).where(
            BroadcastSession.gateway_id == gateway_id,
            BroadcastSession.broadcast_id == payload["broadcast_id"],
        )
    )
    if session_row is None:
        session_row = BroadcastSession(
            gateway_id=gateway_id,
            broadcast_id=payload["broadcast_id"],
            device_mac=payload["device_mac"],
        )
        session.add(session_row)
    session_row.device_name = payload.get("device_name") or session_row.device_name
    session_row.started_at = parse_timestamp(payload.get("broadcast_started_at")) or session_row.started_at
    session_row.last_rssi = payload.get("observed_rssi", session_row.last_rssi)
    session_row.updated_at = _now()
    if payload["event"] == "BROADCAST_ENDED":
        session_row.ended_at = parse_timestamp(payload.get("broadcast_ended_at"))
        session_row.end_detected_at = parse_timestamp(payload.get("end_detected_at"))
        session_row.duration_s = payload.get("broadcast_duration_s")
    return True


def ingest_health(session: Session, payload: dict[str, Any]) -> bool:
    required = ("gateway_id", "event_id")
    if any(not payload.get(key) for key in required):
        raise ValueError(f"gateway_health is missing required field: {required}")
    duplicate = session.scalar(
        select(GatewayHealth.id).where(
            GatewayHealth.gateway_id == payload["gateway_id"],
            GatewayHealth.event_id == payload["event_id"],
        )
    )
    if duplicate is not None:
        return False

    gateway = _upsert_gateway(session, payload)
    gateway.wifi_status = payload.get("wifi")
    gateway.mqtt_status = payload.get("mqtt")
    gateway.sntp_status = payload.get("sntp")
    gateway.sd_ready = payload.get("sd_ready")
    gateway.latest_health = payload
    session.add(GatewayHealth(
        gateway_id=payload["gateway_id"],
        event_id=payload["event_id"],
        uptime_s=payload.get("uptime_s"),
        recorded_at=_now(),
        payload=payload,
    ))
    return True


def ingest_message(session: Session, payload: dict[str, Any]) -> bool:
    message_type = payload.get("message_type")
    if message_type == "broadcast":
        return ingest_broadcast(session, payload)
    if message_type == "gateway_health":
        return ingest_health(session, payload)
    raise ValueError(f"Unsupported message_type: {message_type!r}")
