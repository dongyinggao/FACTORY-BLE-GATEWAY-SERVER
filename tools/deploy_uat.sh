#!/usr/bin/env bash
# Deploy the current committed source to the UAT host without requiring the
# server to access GitHub. The operator supplies SSH/sudo credentials normally.
set -euo pipefail

SERVER_HOST="${1:-ble_gateway@192.168.19.21}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/factory-ble-gateway-server}"
DATABASE_NAME="${DATABASE_NAME:-factory_ble_gateway}"
SERVICE_NAME="${SERVICE_NAME:-factory-ble-gateway-server}"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Refusing to deploy uncommitted changes. Commit or stash them first." >&2
    exit 1
fi

RELEASE_ID="$(git rev-parse --short=12 HEAD)"
LOCAL_ARCHIVE="$(mktemp "/tmp/factory-ble-gateway-server-${RELEASE_ID}.XXXXXX.tar")"
REMOTE_ARCHIVE="/tmp/factory-ble-gateway-server-${RELEASE_ID}.tar"
trap 'rm -f "$LOCAL_ARCHIVE"' EXIT

echo "[1/4] Packaging committed source: ${RELEASE_ID}"
git archive --format=tar --output="$LOCAL_ARCHIVE" HEAD

echo "[2/4] Copying archive to ${SERVER_HOST}"
scp "$LOCAL_ARCHIVE" "${SERVER_HOST}:${REMOTE_ARCHIVE}"

echo "[3/4] Installing source and pending SQL migrations"
ssh "$SERVER_HOST" \
    "REMOTE_APP_DIR='$REMOTE_APP_DIR' REMOTE_ARCHIVE='$REMOTE_ARCHIVE' DATABASE_NAME='$DATABASE_NAME' SERVICE_NAME='$SERVICE_NAME' bash -s" <<'REMOTE'
set -euo pipefail
service_stopped=0
restart_service() {
    if [ "$service_stopped" = "1" ]; then
        sudo systemctl start "$SERVICE_NAME" || true
    fi
}
trap restart_service EXIT

sudo systemctl stop "$SERVICE_NAME"
service_stopped=1
tar -xf "$REMOTE_ARCHIVE" -C "$REMOTE_APP_DIR"

# The ledger prevents already-applied migrations from running again. For an
# existing server that predates this script, seed it once using deploy/README.
sudo -u postgres psql -v ON_ERROR_STOP=1 -d "$DATABASE_NAME" -c \
    'CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());'

for migration in "$REMOTE_APP_DIR"/migrations/*.sql; do
    [ -f "$migration" ] || continue
    name="$(basename "$migration")"
    applied="$(sudo -u postgres psql -At -d "$DATABASE_NAME" -c "SELECT 1 FROM schema_migrations WHERE name = '$name';")"
    if [ "$applied" = "1" ]; then
        echo "migration already applied: $name"
        continue
    fi
    echo "applying migration: $name"
    sudo -u postgres psql -v ON_ERROR_STOP=1 -d "$DATABASE_NAME" -f "$migration"
    sudo -u postgres psql -v ON_ERROR_STOP=1 -d "$DATABASE_NAME" -c \
        "INSERT INTO schema_migrations (name) VALUES ('$name');"
done

sudo systemctl start "$SERVICE_NAME"
service_stopped=0
sleep 2
sudo systemctl is-active --quiet "$SERVICE_NAME"
curl -fsS http://127.0.0.1:8000/healthz
rm -f "$REMOTE_ARCHIVE"
REMOTE

echo "[4/4] UAT deployment completed: ${RELEASE_ID}"
