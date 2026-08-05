# Factory BLE Gateway Server

服务端负责订阅蓝牙网关 MQTT 消息、按 `gateway_id + event_id` 幂等入库，并提供网关、设备和广播记录查询页面。

## Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

默认数据库为本地 SQLite，便于开发。部署时将 `DATABASE_URL` 改为 PostgreSQL URL，并配置服务器 Mosquitto 地址。

访问 `http://127.0.0.1:8000/` 查看网页，`/docs` 查看 API 文档。

## MQTT contract

订阅主题：`factory/product-status/gateway/+/events`。

- `broadcast`：保存原始事件；按 `(gateway_id, event_id)` 去重，并按 `(gateway_id, broadcast_id)` 汇总广播开始、结束和持续时间。
- `gateway_health`：保存健康快照，同时更新网关最新健康状态。

网关只上报本地观察到的广播生命周期。网页按网关展示该观察结果，不将其直接解释为全局在线/离线状态。

## Deployment

UAT 服务器部署、PostgreSQL 初始化、systemd 与 Nginx 配置见
[`deploy/README.md`](deploy/README.md)。OTA 的 `/ota/` 路径与网页共用同一域名，
部署时必须保留该静态文件路径。
