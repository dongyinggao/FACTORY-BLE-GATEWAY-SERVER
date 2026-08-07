import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.database import SessionLocal, initialize_database
from app.ingest import FUSION_IDLE_WINDOW, finalize_stale_global_sessions
from app.models import (
    BroadcastEvent,
    BroadcastSession,
    Device,
    DeviceBroadcastObserver,
    DeviceBroadcastSession,
    Gateway,
    GatewayHealth,
)
from app.mqtt_consumer import GatewayMqttConsumer
from app.settings import settings_from_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
settings = settings_from_env()
consumer = GatewayMqttConsumer(settings)
CHINA_TIMEZONE = ZoneInfo("Asia/Shanghai")
GATEWAY_ONLINE_WINDOW = timedelta(seconds=90)
LONG_BROADCAST_THRESHOLD = timedelta(hours=1)
FREQUENT_BROADCAST_WINDOW = timedelta(minutes=10)
FREQUENT_BROADCAST_COUNT = 5


async def fusion_maintenance_loop() -> None:
    """Close incomplete global sessions even while MQTT is otherwise quiet."""
    while True:
        await asyncio.sleep(5)
        try:
            with SessionLocal.begin() as session:
                finalized = finalize_stale_global_sessions(session)
            if finalized:
                logging.getLogger(__name__).warning(
                    "Finalized %d stale global broadcast session(s)", finalized
                )
        except Exception:
            logging.getLogger(__name__).exception("Global broadcast maintenance failed")


@asynccontextmanager
async def lifespan(application: FastAPI):
    initialize_database()
    consumer.start()
    maintenance_task = asyncio.create_task(fusion_maintenance_loop())
    try:
        yield
    finally:
        maintenance_task.cancel()
        try:
            await maintenance_task
        except asyncio.CancelledError:
            pass
        consumer.stop()


app = FastAPI(title="Factory BLE Gateway Server", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def get_session():
    with SessionLocal() as session:
        yield session


def iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def china_time(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(CHINA_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")


def elapsed_seconds(start: datetime | None, end: datetime) -> int:
    if start is None:
        return 0
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    return max(0, int((end - start).total_seconds()))


def duration_text(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours} 小时 {minutes} 分"
    if minutes:
        return f"{minutes} 分 {seconds} 秒"
    return f"{seconds} 秒"


def broadcast_state(row: DeviceBroadcastSession, now: datetime) -> str:
    if row.ended_at is not None:
        if row.close_reason in ("STALE_TIMEOUT", "END_TIMEOUT"):
            return "incomplete"
        return "ended"
    if row.last_seen_at and now - row.last_seen_at < FUSION_IDLE_WINDOW:
        return "broadcasting"
    return "pending_end"


def broadcast_state_text(row: DeviceBroadcastSession, now: datetime) -> str:
    return {
        "broadcasting": "广播中",
        "ended": "已结束",
        "incomplete": "超时关闭（观测不完整）",
        "pending_end": "等待结束",
    }[broadcast_state(row, now)]


def display_duration(row: DeviceBroadcastSession, now: datetime) -> str:
    return duration_text(row.duration_s if row.duration_s is not None else elapsed_seconds(row.started_at, now))


def gateway_state(gateway: Gateway, now: datetime) -> str:
    if gateway.last_seen_at is None:
        return "offline"
    return "online" if elapsed_seconds(gateway.last_seen_at, now) <= int(GATEWAY_ONLINE_WINDOW.total_seconds()) else "offline"


def china_day_bounds(selected_day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(selected_day, time.min, tzinfo=CHINA_TIMEZONE)
    return start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)


def daily_summary(session: Session, selected_day: date, now: datetime) -> dict:
    day_start, day_end = china_day_bounds(selected_day)
    sessions = session.scalars(
        select(DeviceBroadcastSession)
        .where(DeviceBroadcastSession.started_at >= day_start,
               DeviceBroadcastSession.started_at < day_end)
        .order_by(desc(DeviceBroadcastSession.updated_at))
    ).all()
    gateways = session.scalars(select(Gateway).order_by(Gateway.gateway_id)).all()
    active = [row for row in sessions if broadcast_state(row, now) == "broadcasting"]
    timeouts = [row for row in sessions if row.close_reason in ("STALE_TIMEOUT", "END_TIMEOUT")]
    long_active = [row for row in active if now - _utc(row.started_at) >= LONG_BROADCAST_THRESHOLD]
    per_device: dict[str, dict] = {}
    for row in sessions:
        entry = per_device.setdefault(row.device_mac, {
            "device_mac": row.device_mac,
            "device_name": row.device_name,
            "count": 0,
            "duration_s": 0,
            "latest": row,
            "timestamps": [],
        })
        entry["count"] += 1
        entry["duration_s"] += row.duration_s if row.duration_s is not None else elapsed_seconds(row.started_at, now)
        if row.updated_at > entry["latest"].updated_at:
            entry["latest"] = row
        if row.started_at:
            entry["timestamps"].append(_utc(row.started_at))

    frequent = []
    for entry in per_device.values():
        times = sorted(entry.pop("timestamps"))
        peak_count = 0
        for index, timestamp in enumerate(times):
            window_count = sum(candidate - timestamp <= FREQUENT_BROADCAST_WINDOW
                               for candidate in times[index:])
            peak_count = max(peak_count, window_count)
        if peak_count >= FREQUENT_BROADCAST_COUNT:
            entry["window_count"] = peak_count
            frequent.append(entry)
    offline = [row for row in gateways if gateway_state(row, now) == "offline"]
    return {
        "day_start": day_start,
        "day_end": day_end,
        "sessions": sessions,
        "devices": sorted(per_device.values(), key=lambda item: item["latest"].updated_at, reverse=True),
        "active": active,
        "long_active": long_active,
        "timeouts": timeouts,
        "frequent": frequent,
        "offline": offline,
        "gateway_total": len(gateways),
        "gateway_online": len(gateways) - len(offline),
    }


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


templates.env.filters["cn_time"] = china_time
templates.env.globals["broadcast_state"] = broadcast_state
templates.env.globals["broadcast_state_text"] = broadcast_state_text
templates.env.globals["display_duration"] = display_duration
templates.env.globals["gateway_state"] = gateway_state


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


@app.get("/api/global-broadcasts")
def api_global_broadcasts(
    device_mac: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    query = select(DeviceBroadcastSession).order_by(desc(DeviceBroadcastSession.updated_at)).limit(limit)
    if device_mac:
        query = query.where(DeviceBroadcastSession.device_mac == device_mac.upper())
    return [{
        "device_mac": row.device_mac,
        "device_name": row.device_name,
        "started_at": iso(row.started_at),
        "last_seen_at": iso(row.last_seen_at),
        "ended_at": iso(row.ended_at),
        "duration_s": row.duration_s,
        "close_reason": row.close_reason,
        "time_synced": row.time_synced,
        "observer_count": len(row.observers),
        "observers": [{
            "gateway_id": observer.gateway_id,
            "started_at": iso(observer.started_at),
            "last_seen_at": iso(observer.last_seen_at),
            "ended_at": iso(observer.ended_at),
            "last_rssi": observer.last_rssi,
        } for observer in row.observers],
    } for row in session.scalars(query)]


@app.get("/api/daily")
def api_daily(day: date | None = None, session: Session = Depends(get_session)):
    now = datetime.now(timezone.utc)
    selected_day = day or now.astimezone(CHINA_TIMEZONE).date()
    summary = daily_summary(session, selected_day, now)
    return {
        "day": selected_day.isoformat(),
        "active_devices": len({row.device_mac for row in summary["active"]}),
        "broadcast_sessions": len(summary["sessions"]),
        "gateway_online": summary["gateway_online"],
        "gateway_total": summary["gateway_total"],
        "anomalies": {
            "long_broadcasts": [{"device_mac": row.device_mac, "duration_s": elapsed_seconds(row.started_at, now)} for row in summary["long_active"]],
            "end_timeouts": [{"device_mac": row.device_mac, "close_reason": row.close_reason} for row in summary["timeouts"]],
            "frequent_broadcasts": [{"device_mac": row["device_mac"], "count": row["window_count"]} for row in summary["frequent"]],
            "offline_gateways": [row.gateway_id for row in summary["offline"]],
        },
    }


@app.get("/daily", response_class=HTMLResponse)
def daily_dashboard(request: Request, day: date | None = None,
                    session: Session = Depends(get_session)):
    now = datetime.now(timezone.utc)
    selected_day = day or now.astimezone(CHINA_TIMEZONE).date()
    return templates.TemplateResponse(request, "daily_dashboard.html", {
        "summary": daily_summary(session, selected_day, now),
        "selected_day": selected_day,
        "now": now,
        "long_threshold_s": int(LONG_BROADCAST_THRESHOLD.total_seconds()),
    })


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    now = datetime.now(timezone.utc)
    gateways = session.scalars(select(Gateway).order_by(Gateway.gateway_id)).all()
    broadcasts = session.scalars(
        select(DeviceBroadcastSession).order_by(desc(DeviceBroadcastSession.updated_at)).limit(50)
    ).all()
    active_sessions = [row for row in broadcasts if broadcast_state(row, now) == "broadcasting"]
    pending_sessions = [row for row in broadcasts if broadcast_state(row, now) == "pending_end"]
    today_start = now - timedelta(hours=24)
    stats = {
        "gateway_total": len(gateways),
        "gateway_online": sum(gateway_state(row, now) == "online" for row in gateways),
        "device_total": session.scalar(select(func.count()).select_from(Device)) or 0,
        "broadcast_24h": session.scalar(
            select(func.count()).select_from(DeviceBroadcastSession)
            .where(DeviceBroadcastSession.started_at >= today_start)
        ) or 0,
        "broadcasting_devices": len({row.device_mac for row in active_sessions}),
        "pending_end": len(pending_sessions),
    }
    return templates.TemplateResponse(request, "dashboard.html", {
        "gateways": gateways,
        "broadcasts": broadcasts,
        "active_sessions": active_sessions,
        "stats": stats,
        "now": now,
    })


@app.get("/gateways/{gateway_id}", response_class=HTMLResponse)
def gateway_detail(gateway_id: str, request: Request, session: Session = Depends(get_session)):
    now = datetime.now(timezone.utc)
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
        "now": now,
    })


@app.get("/devices/{device_mac}", response_class=HTMLResponse)
def device_detail(device_mac: str, request: Request, session: Session = Depends(get_session)):
    now = datetime.now(timezone.utc)
    device = session.get(Device, device_mac.upper())
    if device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    broadcasts = session.scalars(
        select(DeviceBroadcastSession).where(DeviceBroadcastSession.device_mac == device.device_mac)
        .order_by(desc(DeviceBroadcastSession.updated_at)).limit(100)
    ).all()
    return templates.TemplateResponse(request, "device_detail.html", {
        "device": device,
        "broadcasts": broadcasts,
        "now": now,
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
