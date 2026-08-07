import json
import logging
from datetime import datetime, timezone
from threading import Lock

import paho.mqtt.client as mqtt

from app.database import SessionLocal
from app.ingest import ingest_message
from app.settings import Settings

LOGGER = logging.getLogger(__name__)


class GatewayMqttConsumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        # The Consumer is a durable MQTT v3.1.1 subscriber.  A fixed client ID
        # plus clean_session=False makes Mosquitto retain this subscription and
        # QoS 1 messages while the web process reconnects after a restart.
        client_options = {
            "client_id": settings.mqtt_client_id,
            "clean_session": False,
        }
        self._manual_ack_enabled = hasattr(mqtt.Client, "ack")
        if self._manual_ack_enabled:
            client_options["manual_ack"] = True
        if hasattr(mqtt, "CallbackAPIVersion"):
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, **client_options)
        else:
            self.client = mqtt.Client(**client_options)
        if not self._manual_ack_enabled:
            LOGGER.warning("Paho MQTT does not support manual acknowledgements; "
                           "upgrade to paho-mqtt 2.x for transaction-bound ACKs")
        if settings.mqtt_username:
            self.client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self._state_lock = Lock()
        self._connected = False
        self._last_changed_at = datetime.now(timezone.utc)
        self._last_reason = "starting"

    def status_snapshot(self) -> dict[str, object]:
        """Return consumer state without exposing broker credentials."""
        with self._state_lock:
            return {
                "connected": self._connected,
                "state": "connected" if self._connected else "disconnected",
                "last_changed_at": self._last_changed_at.isoformat(),
                "reason": self._last_reason,
            }

    def _set_connection_state(self, connected: bool, reason: object | None = None) -> None:
        with self._state_lock:
            self._connected = connected
            self._last_changed_at = datetime.now(timezone.utc)
            self._last_reason = "" if connected else str(reason or "connection unavailable")

    def start(self) -> None:
        self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if getattr(reason_code, "is_failure", reason_code != 0):
            self._set_connection_state(False, reason_code)
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return
        self._set_connection_state(True)
        client.subscribe(self.settings.mqtt_topic, qos=1)
        LOGGER.info("MQTT connected; subscribed %s", self.settings.mqtt_topic)

    def _on_disconnect(self, client, userdata, *args):
        # Callback API v2 supplies (disconnect_flags, reason_code,
        # properties); API v1 only supplies rc.  The last argument in v2 is
        # properties, not the disconnect reason.
        reason_code = args[-2] if len(args) >= 2 else (args[-1] if args else 0)
        self._set_connection_state(False, reason_code)
        LOGGER.warning("MQTT disconnected: %s", reason_code)

    def _on_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("MQTT payload must be a JSON object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            # Invalid input cannot become valid through redelivery. Acknowledge
            # it to avoid blocking the broker's persistent Consumer queue.
            LOGGER.exception("Discarding invalid MQTT payload on topic %s", message.topic)
            self._ack_after_commit(client, message)
            return

        try:
            with SessionLocal.begin() as session:
                accepted = ingest_message(session, payload)
        except Exception:
            # Do not acknowledge before the database transaction succeeds.
            # Mosquitto will redeliver this QoS 1 message to the persistent
            # subscriber after a reconnect; event_id uniqueness makes replay safe.
            LOGGER.exception("Failed to process MQTT topic %s", message.topic)
            return

        self._ack_after_commit(client, message)
        LOGGER.info("MQTT %s %s/%s", "stored" if accepted else "duplicate",
                    payload.get("gateway_id"), payload.get("event_id"))

    @staticmethod
    def _ack_after_commit(client, message) -> None:
        if not hasattr(client, "ack"):
            return
        if message.qos <= 0:
            return
        result = client.ack(message.mid, message.qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.warning("MQTT acknowledgement failed: message_id=%s rc=%s", message.mid, result)
