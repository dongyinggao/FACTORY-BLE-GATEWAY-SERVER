from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    database_url: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    mqtt_topic: str
    mqtt_client_id: str


def settings_from_env() -> Settings:
    return Settings(
        database_url=os.getenv("DATABASE_URL", "sqlite:///./data/ble_gateway.db"),
        mqtt_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        mqtt_port=int(os.getenv("MQTT_PORT", "1883")),
        mqtt_username=os.getenv("MQTT_USERNAME", ""),
        mqtt_password=os.getenv("MQTT_PASSWORD", ""),
        mqtt_topic=os.getenv("MQTT_TOPIC", "factory/product-status/gateway/+/events"),
        mqtt_client_id=os.getenv("MQTT_CLIENT_ID", "factory-ble-gateway-ingest"),
    )
