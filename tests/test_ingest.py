from app.database import Base, create_database_engine
from datetime import datetime, timedelta, timezone

from app.ingest import backfill_global_sessions, finalize_stale_global_sessions, ingest_message
import app.mqtt_consumer as mqtt_consumer
from app.mqtt_consumer import GatewayMqttConsumer
from app.settings import Settings
from app.models import BroadcastSession, DeviceBroadcastObserver, DeviceBroadcastSession, Gateway
from sqlalchemy import delete, select
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
        assert row.last_seen_at.isoformat().startswith("2026-08-05T02:00:20")


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


def _broadcast(gateway_id, event_id, broadcast_id, event, started, *, ended="", detected="", duration=None):
    payload = {
        "message_type": "broadcast", "event_id": event_id, "broadcast_id": broadcast_id,
        "gateway_id": gateway_id, "gateway_location": gateway_id, "event": event,
        "device_mac": "CC:73:07:5F:C1:C5", "device_name": "SM_iCM2", "time_synced": True,
        "recorded_at": detected or started, "broadcast_started_at": started,
        "broadcast_ended_at": ended, "end_detected_at": detected, "observed_rssi": -50,
    }
    if duration is not None:
        payload["broadcast_duration_s"] = duration
    return payload


def test_three_gateway_observations_merge_to_one_global_session():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    with session_factory.begin() as session:
        assert ingest_message(session, _broadcast(
            "GW-01", "gw1-start", "gw1-b1", "BROADCAST_STARTED", "2026-08-05T10:00:00+0800"
        ))
        assert ingest_message(session, _broadcast(
            "GW-02", "gw2-start", "gw2-b1", "BROADCAST_STARTED", "2026-08-05T10:00:03+0800"
        ))
        assert ingest_message(session, _broadcast(
            "GW-03", "gw3-start", "gw3-b1", "BROADCAST_STARTED", "2026-08-05T10:00:07+0800"
        ))

        assert ingest_message(session, _broadcast(
            "GW-01", "gw1-end", "gw1-b1", "BROADCAST_ENDED", "2026-08-05T10:00:00+0800",
            ended="2026-08-05T10:00:20+0800", detected="2026-08-05T10:00:25+0800", duration=20,
        ))
        assert ingest_message(session, _broadcast(
            "GW-02", "gw2-end", "gw2-b1", "BROADCAST_ENDED", "2026-08-05T10:00:03+0800",
            ended="2026-08-05T10:00:21+0800", detected="2026-08-05T10:00:26+0800", duration=18,
        ))
        assert ingest_message(session, _broadcast(
            "GW-03", "gw3-end", "gw3-b1", "BROADCAST_ENDED", "2026-08-05T10:00:07+0800",
            ended="2026-08-05T10:00:22+0800", detected="2026-08-05T10:00:27+0800", duration=15,
        ))

    with session_factory() as session:
        rows = session.scalars(select(DeviceBroadcastSession)).all()
        assert len(rows) == 1
        row = rows[0]
        assert len(row.observers) == 3
        assert row.started_at.isoformat().startswith("2026-08-05T02:00:00")
        assert row.ended_at.isoformat().startswith("2026-08-05T02:00:22")
        assert row.duration_s == 22
        assert session.scalar(select(DeviceBroadcastObserver).where(
            DeviceBroadcastObserver.gateway_id == "GW-02"
        )).last_rssi == -50


def test_two_rounds_from_one_gateway_are_never_merged():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory.begin() as session:
        assert ingest_message(session, _broadcast(
            "GW-01", "round-one", "round-1", "BROADCAST_STARTED", "2026-08-05T10:00:00+0800"
        ))
        assert ingest_message(session, _broadcast(
            "GW-01", "round-two", "round-2", "BROADCAST_STARTED", "2026-08-05T10:00:07+0800"
        ))
    with session_factory() as session:
        rows = session.scalars(select(DeviceBroadcastSession)).all()
        assert len(rows) == 2
        assert all(len(row.observers) == 1 for row in rows)


def test_unsynchronized_events_do_not_merge_and_stale_session_closes():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    base = datetime.now(timezone.utc)
    payload_one = _broadcast(
        "GW-01", "one", "gw1-b1", "BROADCAST_STARTED", base.isoformat()
    )
    payload_two = _broadcast(
        "GW-02", "two", "gw2-b1", "BROADCAST_STARTED", (base + timedelta(seconds=2)).isoformat()
    )
    payload_one["time_synced"] = False
    payload_two["time_synced"] = False
    with session_factory.begin() as session:
        assert ingest_message(session, payload_one)
        assert ingest_message(session, payload_two)
        rows = session.scalars(select(DeviceBroadcastSession)).all()
        assert len(rows) == 2
        now = base + timedelta(seconds=120)
        assert finalize_stale_global_sessions(session, now) == 2
    with session_factory() as session:
        row = session.scalar(select(DeviceBroadcastSession).where(
            DeviceBroadcastSession.ended_at.is_not(None)
        ))
        assert row is not None
        assert row.close_reason == "END_TIMEOUT"


def test_stale_observer_gateway_has_a_distinct_close_reason():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    base = datetime.now(timezone.utc).replace(microsecond=0)

    with session_factory.begin() as session:
        assert ingest_message(session, _broadcast(
            "GW-01", "gw1-start", "gw1-round", "BROADCAST_STARTED", base.isoformat()
        ))
        assert ingest_message(session, _broadcast(
            "GW-02", "gw2-start", "gw2-round", "BROADCAST_STARTED",
            (base + timedelta(seconds=2)).isoformat()
        ))
        assert ingest_message(session, _broadcast(
            "GW-02", "gw2-end", "gw2-round", "BROADCAST_ENDED",
            (base + timedelta(seconds=2)).isoformat(),
            ended=(base + timedelta(seconds=20)).isoformat(),
            detected=(base + timedelta(seconds=25)).isoformat(), duration=18,
        ))
        # GW-01 lost power before reporting its end.  GW-02's normal end is
        # sufficient evidence to avoid labelling the device itself as timed out.
        session.get(Gateway, "GW-01").last_seen_at = base
        assert finalize_stale_global_sessions(session, base + timedelta(seconds=120)) == 1

    with session_factory() as session:
        row = session.scalar(select(DeviceBroadcastSession))
        assert row.close_reason == "OBSERVER_GATEWAY_OFFLINE"
        assert row.ended_at.replace(tzinfo=timezone.utc) == base + timedelta(seconds=20)


def test_backfill_rebuilds_global_sessions_from_existing_local_records():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory.begin() as session:
        started = _broadcast(
            "GW-01", "backfill-start", "backfill-1", "BROADCAST_STARTED", "2026-08-05T10:00:00+0800"
        )
        ended = _broadcast(
            "GW-01", "backfill-end", "backfill-1", "BROADCAST_ENDED", "2026-08-05T10:00:00+0800",
            ended="2026-08-05T10:00:20+0800", detected="2026-08-05T10:00:25+0800", duration=20,
        )
        assert ingest_message(session, started)
        assert ingest_message(session, ended)
        session.execute(delete(DeviceBroadcastObserver))
        session.execute(delete(DeviceBroadcastSession))
        assert backfill_global_sessions(session) == 1
    with session_factory() as session:
        row = session.scalar(select(DeviceBroadcastSession))
        assert row.duration_s == 20
        assert len(row.observers) == 1


def test_activity_refresh_keeps_a_long_broadcast_open():
    engine = create_database_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    base = datetime.now(timezone.utc).replace(microsecond=0)
    started = _broadcast("GW-01", "long-start", "long-1", "BROADCAST_STARTED", base.isoformat())
    active_at = base + timedelta(seconds=60)
    active = _broadcast("GW-01", "long-active", "long-1", "BROADCAST_ACTIVE", base.isoformat(),
                        ended=active_at.isoformat(), duration=60)
    with session_factory.begin() as session:
        assert ingest_message(session, started)
        assert ingest_message(session, active)
        # An activity observation at T+60 keeps the session open at T+120,
        # because the 90-second server timeout has not expired.
        assert finalize_stale_global_sessions(session, base + timedelta(seconds=120)) == 0
    with session_factory() as session:
        row = session.scalar(select(DeviceBroadcastSession))
        assert row.ended_at is None
        assert row.last_seen_at.replace(tzinfo=timezone.utc) == active_at


def test_mqtt_consumer_status_tracks_connect_and_disconnect():
    consumer = GatewayMqttConsumer(Settings(
        database_url="sqlite:///:memory:", mqtt_host="localhost", mqtt_port=1883,
        mqtt_username="", mqtt_password="", mqtt_topic="factory/test", mqtt_client_id="test-client"
    ))
    assert consumer._manual_ack_enabled is True

    class FakeClient:
        def __init__(self):
            self.subscription = None

        def subscribe(self, topic, qos):
            self.subscription = (topic, qos)

    client = FakeClient()
    assert consumer.status_snapshot()["connected"] is False
    consumer._on_connect(client, None, None, 0)
    assert client.subscription == ("factory/test", 1)
    assert consumer.status_snapshot()["connected"] is True
    consumer._on_disconnect(client, None, None, 7, None)
    status = consumer.status_snapshot()
    assert status["connected"] is False
    assert status["reason"] == "7"


def test_mqtt_consumer_acks_only_after_database_commit(monkeypatch):
    consumer = GatewayMqttConsumer(Settings(
        database_url="sqlite:///:memory:", mqtt_host="localhost", mqtt_port=1883,
        mqtt_username="", mqtt_password="", mqtt_topic="factory/test", mqtt_client_id="test-client"
    ))
    completed = []

    class Transaction:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, traceback):
            completed.append(exc_type is None)
            return False

    class SessionFactory:
        @staticmethod
        def begin():
            return Transaction()

    class FakeClient:
        def __init__(self):
            self.acks = []

        def ack(self, message_id, qos):
            assert completed == [True]
            self.acks.append((message_id, qos))
            return mqtt_consumer.mqtt.MQTT_ERR_SUCCESS

    class Message:
        topic = "factory/test"
        payload = b'{"gateway_id":"GW-01","event_id":"event-1"}'
        mid = 42
        qos = 1

    monkeypatch.setattr(mqtt_consumer, "SessionLocal", SessionFactory)
    monkeypatch.setattr(mqtt_consumer, "ingest_message", lambda session, payload: True)
    client = FakeClient()
    consumer._on_message(client, None, Message())
    assert client.acks == [(42, 1)]


def test_mqtt_consumer_does_not_ack_database_failure(monkeypatch):
    consumer = GatewayMqttConsumer(Settings(
        database_url="sqlite:///:memory:", mqtt_host="localhost", mqtt_port=1883,
        mqtt_username="", mqtt_password="", mqtt_topic="factory/test", mqtt_client_id="test-client"
    ))

    class Transaction:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, traceback):
            return False

    class SessionFactory:
        @staticmethod
        def begin():
            return Transaction()

    class FakeClient:
        def ack(self, message_id, qos):
            raise AssertionError("failed database write must not be acknowledged")

    class Message:
        topic = "factory/test"
        payload = b'{"gateway_id":"GW-01","event_id":"event-1"}'
        mid = 43
        qos = 1

    monkeypatch.setattr(mqtt_consumer, "SessionLocal", SessionFactory)
    monkeypatch.setattr(mqtt_consumer, "ingest_message", lambda session, payload: (_ for _ in ()).throw(RuntimeError("db down")))
    consumer._on_message(FakeClient(), None, Message())
