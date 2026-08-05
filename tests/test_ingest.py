from app.database import Base, create_database_engine
from app.ingest import ingest_message
from app.models import BroadcastSession
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker


def test_broadcast_is_idempotent_and_aggregated():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    started = {
        "message_type": "broadcast", "event_id": "boot-1", "broadcast_id": "boot-1",
        "gateway_id": "GW-01", "gateway_location": "Room101", "event": "BROADCAST_STARTED",
        "device_mac": "CC:73:07:5F:C1:C5", "device_name": "SM_iCM2", "time_synced": True,
        "recorded_at": "2026-08-05T10:00:00+0800", "broadcast_started_at": "2026-08-05T10:00:00+0800",
        "broadcast_ended_at": "", "end_detected_at": "", "observed_rssi": -50,
    }
    ended = {**started, "event_id": "boot-2", "event": "BROADCAST_ENDED",
             "recorded_at": "2026-08-05T10:00:25+0800", "broadcast_ended_at": "2026-08-05T10:00:20+0800",
             "end_detected_at": "2026-08-05T10:00:25+0800", "broadcast_duration_s": 20}
    with session_factory.begin() as session:
        assert ingest_message(session, started)
    with session_factory.begin() as session:
        assert not ingest_message(session, started)
        assert ingest_message(session, ended)
    with session_factory() as session:
        row = session.scalar(select(BroadcastSession))
        assert row.duration_s == 20
        assert row.ended_at.isoformat().startswith("2026-08-05T02:00:20")


def test_health_is_idempotent():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    health = {"message_type": "gateway_health", "event_id": "boot-3", "gateway_id": "GW-01",
              "wifi": "Connected", "mqtt": "Connected", "sntp": "Synced", "sd_ready": True}
    with session_factory.begin() as session:
        assert ingest_message(session, health)
    with session_factory.begin() as session:
        assert not ingest_message(session, health)
