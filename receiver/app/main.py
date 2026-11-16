"""Receiver service: idempotent sink for correction orders (疾控库 / 科研库).

* Applies each order exactly once, deduped by the idempotency key
  `order_id` (UNIQUE). A resend after a lost receipt returns the stored
  receipt with `duplicate: true` and does NOT re-apply.
* Application + current-value update happen in one transaction.
* Fault injection (`/fault`) simulates slow responses, unreachable
  receivers, and post-commit disconnects (committed but receipt lost),
  per station or for all stations.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

NAME = os.environ.get("NAME", "receiver")
DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", f"{NAME}.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("receiver")

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS applications (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id      TEXT NOT NULL UNIQUE,
  correction_id TEXT,
  station_id    TEXT NOT NULL,
  day           TEXT NOT NULL,
  value         INTEGER NOT NULL CHECK (value >= 0),
  applied_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS current_values (
  station_id TEXT NOT NULL,
  day        TEXT NOT NULL,
  value      INTEGER NOT NULL,
  updated_at REAL NOT NULL,
  PRIMARY KEY (station_id, day)
);
"""


def conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA busy_timeout=8000")
        _conn.executescript(SCHEMA)
    return _conn


app = FastAPI(
    title=f"接收端 {NAME} (correction receiver)",
    version="1.0.0",
    description="幂等应用更正单；支持故障注入以演示延迟、不可达与提交后断连。",
)


class ApplyIn(BaseModel):
    order_id: str = Field(min_length=1, description="幂等键：同一 order_id 只应用一次")
    correction_id: str | None = None
    station_id: str = Field(min_length=1)
    day: str = Field(min_length=8, max_length=10)
    value: int = Field(ge=0)


@app.post("/apply", summary="应用更正单（幂等）")
async def apply(o: ApplyIn):
    cfg = _consume_fault(o.station_id)
    mode = "none"
    if cfg:
        if cfg["delay_ms"]:
            await asyncio.sleep(cfg["delay_ms"] / 1000)
        mode = cfg["mode"]
        if mode == "fail":
            raise HTTPException(500, "injected failure before commit")
    with _lock:
        c = conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute("SELECT * FROM applications WHERE order_id = ?", (o.order_id,)).fetchone()
            duplicate = row is not None
            if not duplicate:
                now = time.time()
                c.execute(
                    """INSERT INTO applications(order_id, correction_id, station_id, day, value, applied_at)
                       VALUES (?,?,?,?,?,?)""",
                    (o.order_id, o.correction_id, o.station_id, o.day, o.value, now),
                )
                c.execute(
                    """INSERT INTO current_values(station_id, day, value, updated_at) VALUES (?,?,?,?)
                       ON CONFLICT(station_id, day) DO UPDATE
                       SET value = excluded.value, updated_at = excluded.updated_at""",
                    (o.station_id, o.day, o.value, now),
                )
                row = c.execute("SELECT * FROM applications WHERE order_id = ?", (o.order_id,)).fetchone()
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
    if mode == "drop_after_commit":
        # 已提交，但回执在到达发送方前“丢失”。发送方只能重发；幂等键保证只应用一次。
        log.warning("order %s committed but receipt dropped (injected)", o.order_id)
        raise HTTPException(500, "injected receipt loss after commit")
    if duplicate:
        log.info("order %s already applied; returning stored receipt", o.order_id)
    return {
        "status": "applied",
        "order_id": o.order_id,
        "receiver": NAME,
        "seq": row["seq"],
        "duplicate": duplicate,
    }


# ---------------- fault injection (demo) ----------------

_faults: dict[str, dict] = {}  # key: station_id or "*"


class FaultIn(BaseModel):
    station_id: str | None = Field(default=None, description="缺省为 '*'，即全部站点")
    delay_ms: int = Field(default=0, ge=0, le=120000, description="响应前延迟（慢响应）")
    mode: str = Field(
        default="none",
        pattern="^(none|fail|drop_after_commit)$",
        description="fail=提交前失败(不可达)；drop_after_commit=提交后丢回执(断连)",
    )
    times: int | None = Field(default=None, ge=1, description="生效次数，缺省为一直生效")


@app.post("/fault", summary="设置故障注入")
def set_fault(f: FaultIn):
    key = f.station_id or "*"
    _faults[key] = {"delay_ms": f.delay_ms, "mode": f.mode, "times": f.times}
    return {"key": key, **_faults[key]}


@app.get("/fault", summary="查看故障注入配置")
def get_faults():
    return _faults


@app.delete("/fault", summary="清除全部故障注入")
def clear_faults():
    _faults.clear()
    return {"cleared": True}


def _consume_fault(station_id: str) -> dict | None:
    key = station_id if station_id in _faults else ("*" if "*" in _faults else None)
    if key is None:
        return None
    cfg = _faults[key]
    if cfg.get("times") is not None:
        cfg["times"] -= 1
        if cfg["times"] <= 0:
            del _faults[key]
    return dict(cfg)


# ---------------- queries ----------------


@app.get("/health")
def health():
    return {"ok": True, "receiver": NAME}


@app.get("/current", summary="本库现值")
def current_values():
    with _lock:
        rows = conn().execute("SELECT * FROM current_values ORDER BY station_id, day").fetchall()
    return {"receiver": NAME, "current": [dict(r) for r in rows]}


@app.get("/current/{station_id}", summary="某站现值")
def current_for_station(station_id: str):
    with _lock:
        rows = conn().execute(
            "SELECT day, value FROM current_values WHERE station_id = ? ORDER BY day", (station_id,)
        ).fetchall()
    return {"receiver": NAME, "station_id": station_id, "values": {r["day"]: r["value"] for r in rows}}


@app.get("/applications", summary="应用记录（含幂等键，可核对只应用一次）")
def applications():
    with _lock:
        rows = conn().execute("SELECT * FROM applications ORDER BY seq").fetchall()
    return {"receiver": NAME, "applications": [dict(r) for r in rows]}
