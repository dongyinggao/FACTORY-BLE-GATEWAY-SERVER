import json
import logging

import paho.mqtt.client as mqtt

from app.database import SessionLocal
from app.ingest import ingest_message
from app.settings import Settings

LOGGER = logging.getLogger(__name__)


class GatewayMqttConsumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=settings.mqtt_client_id)
        if settings.mqtt_username:
            self.client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def start(self) -> None:
        self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return
        client.subscribe(self.settings.mqtt_topic, qos=1)
        LOGGER.info("MQTT connected; subscribed %s", self.settings.mqtt_topic)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        LOGGER.warning("MQTT disconnected: %s", reason_code)

    def _on_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("MQTT payload must be a JSON object")
            with SessionLocal.begin() as session:
                accepted = ingest_message(session, payload)
            LOGGER.info("MQTT %s %s/%s", "stored" if accepted else "duplicate", payload.get("gateway_id"), payload.get("event_id"))
        except Exception:
            LOGGER.exception("Failed to process MQTT topic %s", message.topic)
