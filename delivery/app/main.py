"""Delivery API: accept water-quality corrections, fan them out reliably."""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import date

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import core, db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

dispatcher = core.Dispatcher()


@asynccontextmanager
async def lifespan(_):
    core.init_schema()
    await dispatcher.start()
    yield
    await dispatcher.stop()


app = FastAPI(
    title="水质更正投递服务 (correction delivery)",
    version="1.0.0",
    description="受理站点日菌落数更正，向疾控库/科研库可靠、保序、幂等地投递。受理即返回，不等待下游。",
    lifespan=lifespan,
)


class CorrectionIn(BaseModel):
    station_id: str = Field(min_length=1, max_length=64, examples=["ST-001"], description="站号")
    day: date = Field(examples=["2026-11-16"], description="UTC 日期 YYYY-MM-DD")
    value: int = Field(ge=0, le=10**12, examples=[123], description="菌落数（非负整数）")


@app.get("/health")
def health():
    return {"ok": True, "instance": core.INSTANCE_ID}


if os.environ.get("CHAOS_CRASH") == "1":
    # 演示/自测用混沌开关：调用后本进程立即硬退出（os._exit，无优雅停机），
    # 由 compose 的 restart 策略或外部守护拉起，用于演示"退出重启后自动续送"。
    @app.post("/admin/crash", summary="[演示] 让本实例立即崩溃退出")
    def crash():
        threading.Timer(0.3, lambda: os._exit(1)).start()
        return {"crashing": True, "instance": core.INSTANCE_ID}


@app.post("/corrections", status_code=202, summary="提交更正（保存现值并生成两库更正单，受理即返回）")
def submit_correction(body: CorrectionIn):
    cid = core.accept_correction(body.station_id, body.day.isoformat(), body.value)
    return get_correction(cid) | {"status": "accepted"}


@app.get("/corrections", summary="最近更正列表")
def list_corrections(limit: int = Query(50, le=500)):
    rows = db.query("SELECT * FROM corrections ORDER BY rowid DESC LIMIT ?", (limit,))
    return {"corrections": [dict(r) for r in rows]}


@app.get("/corrections/{cid}", summary="单笔更正及两库送达状态")
def get_correction(cid: str):
    corr = db.query_one("SELECT * FROM corrections WHERE id = ?", (cid,))
    if corr is None:
        raise HTTPException(404, "correction not found")
    orders = db.query("SELECT * FROM orders WHERE correction_id = ? ORDER BY target", (cid,))
    return {"correction": dict(corr), "orders": [dict(o) for o in orders]}


@app.get("/orders", summary="按条件查询更正单（target/station_id/status）")
def list_orders(
    target: str | None = None,
    station_id: str | None = None,
    status: str | None = None,
    limit: int = Query(100, le=1000),
):
    sql, args = "SELECT * FROM orders WHERE 1=1", []
    if target:
        sql += " AND target = ?"
        args.append(target)
    if station_id:
        sql += " AND station_id = ?"
        args.append(station_id)
    if status:
        sql += " AND status = ?"
        args.append(status)
    sql += " ORDER BY seq DESC LIMIT ?"
    args.append(limit)
    return {"orders": [dict(r) for r in db.query(sql, args)]}


@app.get("/orders/{oid}", summary="单张更正单详情（含回执）")
def get_order(oid: str):
    r = db.query_one("SELECT * FROM orders WHERE id = ?", (oid,))
    if r is None:
        raise HTTPException(404, "order not found")
    return dict(r)


@app.get("/current", summary="投递服务保存的现值")
def current_values():
    rows = db.query("SELECT * FROM current_values ORDER BY station_id, day")
    return {"current": [dict(r) for r in rows]}


@app.get("/channels", summary="通道租约（可观测双实例分工）")
def channels():
    rows = db.query("SELECT * FROM channels ORDER BY id")
    return {"instance": core.INSTANCE_ID, "channels": [dict(r) for r in rows]}


async def _fetch(client: httpx.AsyncClient, base: str, path: str):
    try:
        r = await client.get(base + path)
        return r.json()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@app.get("/targets/current", summary="两库现值（实时拉取两个接收端）")
async def targets_current():
    async with httpx.AsyncClient(timeout=2.5) as client:
        cdc, res = await asyncio.gather(
            _fetch(client, core.CDC_URL, "/current"),
            _fetch(client, core.RESEARCH_URL, "/current"),
        )
    return {"cdc": cdc, "research": res}


@app.get("/targets/{target}/applications", summary="某库应用记录（实时拉取接收端）")
async def target_applications(target: str):
    base = core.TARGET_URLS.get(target)
    if base is None:
        raise HTTPException(404, "unknown target; use 'cdc' or 'research'")
    async with httpx.AsyncClient(timeout=2.5) as client:
        return await _fetch(client, base, "/applications")
