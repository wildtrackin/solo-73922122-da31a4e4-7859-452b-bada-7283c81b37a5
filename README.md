# 水质更正投递系统

水质数仓工程师更正"站日菌落数"后，由本系统把更正**可靠、保序、幂等**地分别投递到
**疾控库（cdc）** 和 **科研库（research）**，避免回执丢失导致的重复应用。

- **投递服务 delivery**（FastAPI + SQLite，双实例并发）：受理更正、保存现值、为两库各生成一张更正单，后台异步投递（受理不等待下游）。
- **接收服务 receiver**（FastAPI + SQLite，两个独立持久库）：幂等应用更正单，维护本库现值与应用记录，支持故障注入演示。

## 快速开始

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| delivery-1 | http://localhost:8000/docs | 投递实例 1（含 `/admin/crash` 崩溃演示端点） |
| delivery-2 | http://localhost:8001/docs | 投递实例 2 |
| receiver-cdc | http://localhost:8002/docs | 疾控库接收端（独立持久卷 `cdc-data`） |
| receiver-research | http://localhost:8003/docs | 科研库接收端（独立持久卷 `research-data`） |

两个投递实例共享同一 SQLite 持久卷（`delivery-shared`，同一宿主机，WAL 模式下安全），
通过**通道租约**协调分工；任一实例退出/重启后自动续送。

无 Docker 时可用 `scripts/dev_local.sh` 以相同拓扑本地运行（端口 8000-8003）。

## 一致性设计（逐条对应需求）

| 需求 | 实现 |
|---|---|
| 受理不等待下游 | `POST /corrections` 在一个 SQLite 事务内写入现值 + 两库更正单即返回 202，投递全在后台 |
| 每库同站按受理顺序应用，未确认前不投下一单 | 通道 = (库, 站)。每通道一个 worker，严格按 `orders.seq`（受理序）逐单发送，收到有效回执并落库后才发下一单 |
| 某库某站慢/不可达不阻塞其他 | 每通道独立 async worker + 指数退避；慢通道只占用自己的 worker |
| 已提交但回执丢失，重发只应用一次 | 更正单 id 即幂等键；接收端 `order_id UNIQUE`，重复投递返回原回执 `duplicate:true`，不重复应用 |
| 双实例并发 / 退出重启自动续送 | 通道租约（原子 `UPDATE` 抢占，15s 过期，工作中续租）；崩溃实例的租约过期后由存活实例接管；启动时 `inflight→pending` 重排队（接收端幂等保证重发安全） |
| 不漏单 | 受理即持久化（事务）；未确认单永久重试 |
| 不倒退现值 | 现值只在受理时按受理序写入；接收端按通道受理序应用 |
| 未确认不报送达 | `confirmed` 仅在收到匹配回执并事务落库后设置；故障中查询可见 `pending/inflight` 与 `attempts/last_error` |

## /docs 演示

**1. 提交更正**（delivery-1 `:8000/docs` → `POST /corrections`）：

```json
{"station_id": "ST-001", "day": "2026-11-16", "value": 123}
```

返回 202 与两张更正单；随后 `GET /corrections/{id}` 可见两库各自 `confirmed` 及回执。

**2. 慢响应不阻塞**（receiver-cdc `:8002/docs` → `POST /fault`）：

```json
{"station_id": "ST-001", "delay_ms": 3000}
```

再对 ST-001 与 ST-002 各提交一笔更正：ST-002 与科研库立即确认，ST-001 在疾控库延迟后确认。

**3. 提交后断连（回执丢失）**（receiver-research → `POST /fault`）：

```json
{"station_id": "ST-003", "mode": "drop_after_commit", "times": 1}
```

提交 ST-003 更正：接收端**已提交**但回执"丢失"（对发送方表现为 500/断连）；投递方自动重发，
接收端幂等去重——`GET /applications` 中该单只有一条记录，回执 `duplicate:true`。

**4. 实例崩溃续送**（delivery-1）：先提交几笔更正，然后 `POST /admin/crash`（或用
`docker compose restart delivery-1`）。实例硬退出后由 restart 策略拉起，
`GET /health` 可见实例 ID 已变，积压单继续送完，无漏单、无重复。

**5. 查询**：`GET /current`（投递侧现值）、`GET /targets/current`（两库现值）、
`GET /targets/{target}/applications`（两库应用记录）、`GET /channels`（租约分工）。

故障注入：`mode=fail`（提交前失败，模拟不可达）、`delay_ms`（慢响应）、
`drop_after_commit`（提交后丢回执）；`station_id` 缺省为 `*`（全部站点），
`times` 限次数；`DELETE /fault` 清除。

## API 一览（投递服务）

- `POST /corrections` 提交更正（站号、UTC 日期、非负整数菌落数）→ 202
- `GET /corrections` / `GET /corrections/{cid}` 更正及两库送达状态
- `GET /orders` / `GET /orders/{oid}` 更正单查询（target/station_id/status 过滤）
- `GET /current` 投递侧现值；`GET /targets/current` 两库现值
- `GET /targets/{target}/applications` 某库应用记录
- `GET /channels` 通道租约；`GET /health` 健康与实例 ID

## 自测

`tests/selftest.py` 端到端覆盖：参数校验、基本投递、慢站不阻塞、回执丢失只应用一次、
接收端故障与恢复、双实例突发 24 笔、同站 12 笔保序、实例崩溃后续送不漏不重不倒退。

```bash
# 对 compose 部署运行（delivery-1 已开 CHAOS_CRASH=1）
RESTART_CMD="curl -sf -X POST http://127.0.0.1:8000/admin/crash" python3 tests/selftest.py
```

本仓库提交前已在同等拓扑（2 投递 + 2 接收，本地进程）下跑通全部 38 项断言。

## 说明与限制

- 双投递实例共享 SQLite 文件依赖**同一宿主机**的卷（compose 命名卷满足）；跨主机部署需改用
  client-server 数据库（表结构与租约逻辑不变）。
- "提交后断连"以"提交后返回 500"模拟；对投递方而言与 TCP 断连/超时等价（都收不到回执，都会重发）。
- 回执以 JSON 校验（`order_id` 匹配 + `status=applied`）后才确认；未鉴权，仅供内网演示。
- 时间戳为 epoch 秒（float）；租约依赖宿主机时钟（单主机部署无漂移问题）。
