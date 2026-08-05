import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.database import SessionLocal, initialize_database
from app.models import BroadcastEvent, BroadcastSession, Device, Gateway, GatewayHealth
from app.mqtt_consumer import GatewayMqttConsumer
from app.settings import settings_from_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
settings = settings_from_env()
consumer = GatewayMqttConsumer(settings)


@asynccontextmanager
async def lifespan(application: FastAPI):
    initialize_database()
    consumer.start()
    yield
    consumer.stop()


app = FastAPI(title="Factory BLE Gateway Server", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def get_session():
    with SessionLocal() as session:
        yield session


def iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


@app.get("/healthz")
def healthz():
    return {"status": "ok", "mqtt_topic": settings.mqtt_topic}


@app.get("/api/gateways")
def api_gateways(session: Session = Depends(get_session)):
    rows = session.scalars(select(Gateway).order_by(Gateway.gateway_id)).all()
    return [{
        "gateway_id": row.gateway_id,
        "location": row.location,
        "last_seen_at": iso(row.last_seen_at),
        "wifi": row.wifi_status,
        "mqtt": row.mqtt_status,
        "sntp": row.sntp_status,
        "sd_ready": row.sd_ready,
    } for row in rows]


@app.get("/api/broadcasts")
def api_broadcasts(
    gateway_id: str | None = None,
    device_mac: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    query = select(BroadcastSession).order_by(desc(BroadcastSession.updated_at)).limit(limit)
    if gateway_id:
        query = query.where(BroadcastSession.gateway_id == gateway_id)
    if device_mac:
        query = query.where(BroadcastSession.device_mac == device_mac.upper())
    return [{
        "gateway_id": row.gateway_id,
        "broadcast_id": row.broadcast_id,
        "device_mac": row.device_mac,
        "device_name": row.device_name,
        "started_at": iso(row.started_at),
        "ended_at": iso(row.ended_at),
        "duration_s": row.duration_s,
        "last_rssi": row.last_rssi,
        "updated_at": iso(row.updated_at),
    } for row in session.scalars(query)]


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    gateways = session.scalars(select(Gateway).order_by(Gateway.gateway_id)).all()
    broadcasts = session.scalars(
        select(BroadcastSession).order_by(desc(BroadcastSession.updated_at)).limit(20)
    ).all()
    return templates.TemplateResponse(request, "dashboard.html", {
        "gateways": gateways,
        "broadcasts": broadcasts,
        "now": datetime.now(timezone.utc),
    })


@app.get("/gateways/{gateway_id}", response_class=HTMLResponse)
def gateway_detail(gateway_id: str, request: Request, session: Session = Depends(get_session)):
    gateway = session.get(Gateway, gateway_id)
    if gateway is None:
        raise HTTPException(status_code=404, detail="Gateway not found")
    broadcasts = session.scalars(
        select(BroadcastSession)
        .where(BroadcastSession.gateway_id == gateway_id)
        .order_by(desc(BroadcastSession.updated_at)).limit(100)
    ).all()
    health = session.scalars(
        select(GatewayHealth).where(GatewayHealth.gateway_id == gateway_id)
        .order_by(desc(GatewayHealth.recorded_at)).limit(20)
    ).all()
    return templates.TemplateResponse(request, "gateway_detail.html", {
        "gateway": gateway,
        "broadcasts": broadcasts,
        "health": health,
    })


@app.get("/devices/{device_mac}", response_class=HTMLResponse)
def device_detail(device_mac: str, request: Request, session: Session = Depends(get_session)):
    device = session.get(Device, device_mac.upper())
    if device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    broadcasts = session.scalars(
        select(BroadcastSession).where(BroadcastSession.device_mac == device.device_mac)
        .order_by(desc(BroadcastSession.updated_at)).limit(100)
    ).all()
    return templates.TemplateResponse(request, "device_detail.html", {
        "device": device,
        "broadcasts": broadcasts,
    })


@app.get("/api/events/{gateway_id}")
def api_events(gateway_id: str, limit: int = Query(default=100, ge=1, le=500), session: Session = Depends(get_session)):
    events = session.scalars(
        select(BroadcastEvent).where(BroadcastEvent.gateway_id == gateway_id)
        .order_by(desc(BroadcastEvent.received_at)).limit(limit)
    ).all()
    return [{
        "event_id": event.event_id,
        "broadcast_id": event.broadcast_id,
        "event": event.event_type,
        "device_mac": event.device_mac,
        "recorded_at": iso(event.recorded_at),
        "received_at": iso(event.received_at),
    } for event in events]
