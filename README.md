# Factory BLE Gateway Server

服务端负责订阅蓝牙网关 MQTT 消息、按 `gateway_id + event_id` 幂等入库，并提供网关、设备和广播记录查询页面。它保留每台网关的原始观测，同时将同一 MAC 的多网关观测融合为一轮全局广播会话。

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

- `broadcast`：保存原始事件；按 `(gateway_id, event_id)` 去重，并按 `(gateway_id, broadcast_id)` 汇总本机广播开始、结束和持续时间。
- `gateway_health`：保存健康快照，同时更新网关最新健康状态。

## Multi-gateway fusion

网关只上报本地观察到的广播生命周期，不直接判断设备全局在线或离线。服务端按以下规则融合：

- 设备身份是 `device_mac`；设备名称仅用于显示。
- 已校时的 `BROADCAST_STARTED` 在相差 10 秒内视为同一轮全局广播；多个网关成为该轮的观测节点。
- 全局开始时间取最早首包时间，最后广播时间取所有节点的最晚末包时间，持续时长据此计算。
- 仅当全部观测节点结束时关闭全局会话。若已有节点正常结束、其余节点所属网关在 90 秒内失联，则以 `OBSERVER_GATEWAY_OFFLINE` 关闭并保留“部分观测缺失”语义；没有正常结束证据时，最后观测后 90 秒以 `END_TIMEOUT` 安全关闭。
- `time_synced=false` 的事件不跨网关合并，避免使用 Broker 接收时间造成错误融合。

页面首页和设备详情显示全局融合会话；网关详情仍显示该网关的原始本机观察。`GET /api/broadcasts` 保持返回本机会话，`GET /api/global-broadcasts` 返回融合后的会话与观测节点。

新版网关对持续超过 60 秒的广播每分钟发送一次 `BROADCAST_ACTIVE`。该消息只刷新服务器的
`last_seen_at`，不替代开始/结束的可靠事件；服务端在最后活动观测后 90 秒仍未收到更新时标记
`END_TIMEOUT`，页面显示为“结束未确认（观测不完整）”，而非正常结束。页面顶部还显示服务端 MQTT Consumer 的实时连接状态；Broker 中断时会提示数据可能不是最新。

## Daily dashboard

`/daily` 提供按中国标准时间统计的每日看板，展示当日有广播的去重设备数、全局广播轮次、
网关在线数、长广播（默认 1 小时）、结束超时、网关中断影响观测、高频广播和离线网关。`GET /api/daily` 提供同一
聚合数据。这里的“当日有广播设备”不等同于设备业务在线状态。

## Deployment

UAT 服务器部署、PostgreSQL 初始化、systemd 与 Nginx 配置见
[`deploy/README.md`](deploy/README.md)。OTA 的 `/ota/` 路径与网页共用同一域名，
部署时必须保留该静态文件路径。
