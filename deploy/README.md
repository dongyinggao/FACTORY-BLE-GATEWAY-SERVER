# UAT Server Deployment

The UAT host already runs Mosquitto and serves OTA files. This application is a
separate MQTT consumer and web process; it must not replace Mosquitto or the
`/ota/` Nginx location.

## 1. Install runtime dependencies

Run on `blegatewayserver` with an administrator account:

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib python3-venv nginx
sudo -u postgres createuser --pwprompt ble_gateway_app
sudo -u postgres createdb -O ble_gateway_app factory_ble_gateway
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

The UAT server currently cannot reach GitHub. Package and upload the committed
repository from the development computer instead:

```bash
git archive --format=tar --output=/tmp/factory-ble-gateway-server.tar HEAD
scp /tmp/factory-ble-gateway-server.tar ble_gateway@192.168.19.21:/tmp/
ssh ble_gateway@192.168.19.21 'mkdir -p /opt/factory-ble-gateway-server && tar -xf /tmp/factory-ble-gateway-server.tar -C /opt/factory-ble-gateway-server'
```

If the server cannot access PyPI, use the Ubuntu-packaged runtime instead:

```bash
sudo apt install -y python3-fastapi python3-sqlalchemy python3-paho-mqtt python3-uvicorn python3-psycopg2 python3-jinja2
sudo rm -rf /opt/factory-ble-gateway-server/.venv
sudo -u ble_gateway python3 -m venv --system-site-packages /opt/factory-ble-gateway-server/.venv
```

Create `/etc/factory-ble-gateway-server.env`:

```ini
# PyPI runtime: postgresql+psycopg://...
# Ubuntu package fallback (SQLAlchemy 1.4): postgresql+psycopg2://...
DATABASE_URL=postgresql+psycopg2://ble_gateway_app:<database-password>@127.0.0.1:5432/factory_ble_gateway
# Current UAT Mosquitto listener is bound to this server interface, not loopback.
MQTT_HOST=192.168.19.21
MQTT_PORT=1883
MQTT_TOPIC=factory/product-status/gateway/+/events
MQTT_CLIENT_ID=factory-ble-gateway-ingest-uat
```

Then run `sudo chmod 600 /etc/factory-ble-gateway-server.env` and
`sudo chown root:root /etc/factory-ble-gateway-server.env`.

Initialize the schema and grant the application account access:

```bash
sudo -u postgres psql -d factory_ble_gateway -f migrations/001_initial.sql
sudo -u postgres psql -d factory_ble_gateway -f migrations/002_multi_gateway_fusion.sql
sudo -u postgres psql -d factory_ble_gateway -f migrations/003_fusion_one_observer_per_gateway.sql
sudo -u postgres psql -d factory_ble_gateway -c 'GRANT USAGE ON SCHEMA public TO ble_gateway_app; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ble_gateway_app; GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO ble_gateway_app;'
```

After introducing migration `002`, run the repeatable historical backfill once
while the consumer is stopped. It links old per-gateway sessions into global
sessions without altering raw MQTT events:

```bash
sudo systemctl stop factory-ble-gateway-server
sudo -u ble_gateway /opt/factory-ble-gateway-server/.venv/bin/python -m app.backfill_fusion
sudo systemctl start factory-ble-gateway-server
```

## 3. Start and verify

```bash
sudo cp deploy/factory-ble-gateway-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now factory-ble-gateway-server
sudo systemctl status factory-ble-gateway-server
curl http://127.0.0.1:8000/healthz
```

The supplied Nginx file is a complete UAT virtual host: it preserves `/ota/`
and proxies all other routes to the web service. Back up the existing host
configuration, then test before reload: `sudo nginx -t && sudo systemctl reload nginx`.

## 4. Acceptance

1. Confirm `systemctl status` reports the consumer running.
2. Generate a real gateway broadcast and health event.
3. Verify `journalctl -u factory-ble-gateway-server -f` shows `stored`.
4. Open `https://ble-gateway-uat.singularmedical.net/` and check the gateway,
   device MAC, global broadcast start/last-seen/duration and observer nodes.
5. Restart the Broker or publish the same MQTT message twice: only one database
   row may exist for `(gateway_id, event_id)`.
6. Publish start/end records for the same MAC from three gateways within ten
   seconds. The dashboard must show one global broadcast with three observer
   nodes, while each gateway page continues to show its own local record.
