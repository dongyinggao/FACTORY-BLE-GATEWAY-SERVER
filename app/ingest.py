import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    BroadcastEvent,
    BroadcastSession,
    Device,
    DeviceBroadcastObserver,
    DeviceBroadcastSession,
    Gateway,
    GatewayHealth,
)

LOGGER = logging.getLogger(__name__)

# Different gateways do not receive the first packet at exactly the same
# instant.  Ten seconds safely absorbs scan/radio jitter while remaining far
# below the expected interval between separate product broadcasts.
FUSION_START_WINDOW = timedelta(seconds=10)
# A missing BROADCAST_ENDED must not leave a global session open forever.  This
# is a server-side safety net; it does not replace the gateway's 5 s detector.
FUSION_IDLE_WINDOW = timedelta(seconds=15)


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


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalize database values from PostgreSQL and timezone-naive SQLite tests."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _event_time(payload: dict[str, Any], field: str) -> datetime | None:
    """Return an event timestamp only when the gateway declared it synchronized."""
    if not payload.get("time_synced", False):
        return None
    return parse_timestamp(payload.get(field))


def _recompute_global_session(global_session: DeviceBroadcastSession) -> None:
    observers = global_session.observers
    starts = [_as_utc(row.started_at) for row in observers if row.started_at is not None]
    last_seen = [_as_utc(row.last_seen_at) for row in observers if row.last_seen_at is not None]
    detected = [_as_utc(row.end_detected_at) for row in observers if row.end_detected_at is not None]

    if starts:
        global_session.started_at = min(starts)
    if last_seen:
        global_session.last_seen_at = max(last_seen)

    if observers and all(row.ended_at is not None for row in observers):
        global_session.ended_at = global_session.last_seen_at
        global_session.end_detected_at = max(detected) if detected else global_session.ended_at
        if global_session.started_at and global_session.ended_at:
            global_session.duration_s = max(
                0, int((global_session.ended_at - global_session.started_at).total_seconds())
            )
    else:
        # A delayed START from another gateway may legitimately reopen a
        # session that was previously closed from incomplete information.
        global_session.ended_at = None
        global_session.end_detected_at = None
        global_session.duration_s = None
    global_session.updated_at = _now()


def _find_or_create_global_session(
    session: Session,
    *,
    gateway_id: str,
    broadcast_id: str,
    device_mac: str,
    device_name: str | None,
    started_at: datetime | None,
    time_synced: bool,
) -> DeviceBroadcastSession:
    existing_observer = session.scalar(
        select(DeviceBroadcastObserver).where(
            DeviceBroadcastObserver.gateway_id == gateway_id,
            DeviceBroadcastObserver.gateway_broadcast_id == broadcast_id,
        )
    )
    if existing_observer is not None:
        return existing_observer.global_session

    global_session = None
    if time_synced and started_at is not None:
        global_session = session.scalar(
            select(DeviceBroadcastSession)
            .where(
                DeviceBroadcastSession.device_mac == device_mac,
                DeviceBroadcastSession.time_synced.is_(True),
                DeviceBroadcastSession.started_at >= started_at - FUSION_START_WINDOW,
                DeviceBroadcastSession.started_at <= started_at + FUSION_START_WINDOW,
            )
            .order_by(DeviceBroadcastSession.updated_at.desc())
        )
    if global_session is None:
        # Unsynchronized timestamps intentionally create a standalone
        # session: merging them by broker receive time would be misleading.
        global_session = DeviceBroadcastSession(
            device_mac=device_mac,
            device_name=device_name,
            started_at=started_at,
            last_seen_at=started_at,
            time_synced=time_synced,
            updated_at=_now(),
        )
        session.add(global_session)
        session.flush()
    return global_session


def _update_global_observation(
    session: Session,
    payload: dict[str, Any],
    local_session: BroadcastSession,
) -> None:
    started_at = local_session.started_at
    global_session = _find_or_create_global_session(
        session,
        gateway_id=payload["gateway_id"],
        broadcast_id=payload["broadcast_id"],
        device_mac=payload["device_mac"],
        device_name=payload.get("device_name"),
        started_at=started_at,
        time_synced=bool(payload.get("time_synced", False)) and started_at is not None,
    )
    observer = session.scalar(
        select(DeviceBroadcastObserver).where(
            DeviceBroadcastObserver.gateway_id == payload["gateway_id"],
            DeviceBroadcastObserver.gateway_broadcast_id == payload["broadcast_id"],
        )
    )
    if observer is None:
        observer = DeviceBroadcastObserver(
            global_session_id=global_session.id,
            gateway_id=payload["gateway_id"],
            gateway_broadcast_id=payload["broadcast_id"],
            time_synced=bool(payload.get("time_synced", False)) and started_at is not None,
        )
        session.add(observer)
        global_session.observers.append(observer)
    observer.started_at = local_session.started_at or observer.started_at
    observer.last_seen_at = local_session.ended_at or local_session.started_at or observer.last_seen_at
    observer.last_rssi = local_session.last_rssi
    observer.updated_at = _now()
    if payload["event"] == "BROADCAST_ENDED":
        observer.ended_at = local_session.ended_at
        observer.end_detected_at = local_session.end_detected_at
    global_session.device_name = payload.get("device_name") or global_session.device_name
    session.flush()
    _recompute_global_session(global_session)


def finalize_stale_global_sessions(session: Session, now: datetime | None = None) -> int:
    """Close sessions whose expected end event was never delivered.

    The function is deliberately idempotent and is called from the consumer's
    periodic maintenance task as well as during new event ingestion.
    """
    now = now or _now()
    rows = session.scalars(
        select(DeviceBroadcastSession).where(DeviceBroadcastSession.ended_at.is_(None))
    ).all()
    closed = 0
    for row in rows:
        last_seen_at = _as_utc(row.last_seen_at)
        if last_seen_at is None or now - last_seen_at < FUSION_IDLE_WINDOW:
            continue
        row.ended_at = last_seen_at
        row.end_detected_at = now
        if row.started_at:
            row.duration_s = max(
                0, int((row.ended_at - _as_utc(row.started_at)).total_seconds())
            )
        row.updated_at = now
        closed += 1
    return closed


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
    _update_global_observation(session, payload, session_row)
    finalize_stale_global_sessions(session)
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
