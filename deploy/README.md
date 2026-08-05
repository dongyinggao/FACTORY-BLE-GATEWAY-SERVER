# UAT Server Deployment

The UAT host already runs Mosquitto and serves OTA files. This application is a
separate MQTT consumer and web process; it must not replace Mosquitto or the
`/ota/` Nginx location.

## 1. Install runtime dependencies

Run on `blegatewayserver` with an administrator account:

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib python3-venv nginx
sudo -u postgres createuser --pwprompt ble_gateway
sudo -u postgres createdb -O ble_gateway ble_gateway
```

Use a generated database password. Store it only in
`/etc/factory-ble-gateway-server.env`, which must be mode `0600`.

## 2. Install the application

```bash
sudo mkdir -p /opt/factory-ble-gateway-server
sudo chown ble_gateway:ble_gateway /opt/factory-ble-gateway-server
git clone <server-repository-url> /opt/factory-ble-gateway-server
sudo -u ble_gateway python3 -m venv /opt/factory-ble-gateway-server/.venv
sudo -u ble_gateway /opt/factory-ble-gateway-server/.venv/bin/pip install -r /opt/factory-ble-gateway-server/requirements.txt
```

Create `/etc/factory-ble-gateway-server.env`:

```ini
DATABASE_URL=postgresql+psycopg://ble_gateway:<database-password>@127.0.0.1:5432/ble_gateway
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_TOPIC=factory/product-status/gateway/+/events
MQTT_CLIENT_ID=factory-ble-gateway-ingest-uat
```

Then run `sudo chmod 600 /etc/factory-ble-gateway-server.env` and
`sudo chown root:root /etc/factory-ble-gateway-server.env`.

## 3. Start and verify

```bash
sudo cp deploy/factory-ble-gateway-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now factory-ble-gateway-server
sudo systemctl status factory-ble-gateway-server
curl http://127.0.0.1:8000/healthz
```

Merge `deploy/nginx-factory-ble-gateway-server.conf` with the existing UAT
server block, preserving its current certificate paths and OTA location. Test
before reload: `sudo nginx -t && sudo systemctl reload nginx`.

## 4. Acceptance

1. Confirm `systemctl status` reports the consumer running.
2. Generate a real gateway broadcast and health event.
3. Verify `journalctl -u factory-ble-gateway-server -f` shows `stored`.
4. Open `https://ble-gateway-uat.singularmedical.net/` and check the gateway,
   device MAC, broadcast start/end/duration.
5. Restart the Broker or publish the same MQTT message twice: only one database
   row may exist for `(gateway_id, event_id)`.
