"""Acceptance and reliable, ordered, exactly-once delivery of corrections.

Design notes
------------
* Acceptance (`accept_correction`) is a single SQLite transaction: it stores
  the new current value plus one delivery order per target DB. It never
  touches the network, so POST /corrections returns immediately.
* Delivery is per channel = (target DB, station). Inside a channel, orders
  are sent strictly in acceptance order (`orders.seq`), one in flight at a
  time: the next order is only sent after the previous one is confirmed by a
  valid receipt. A slow/unreachable channel therefore never blocks other
  stations, the other DB, or newly accepted corrections.
* Exactly-once application: the order id is the idempotency key. Receivers
  dedupe by it, so re-sending after a lost receipt is applied exactly once.
* Multi-instance: both delivery instances share the SQLite file. A channel is
  claimed with an atomic lease UPDATE; a crashed instance's leases expire and
  are picked up by the survivor / by itself after restart. On startup,
  orders left 'inflight' are requeued — they were never confirmed, and the
  receiver dedupes the resend.
* An order is marked 'confirmed' only after a valid receipt is persisted.
  Nothing is ever reported delivered without a receipt.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid

import httpx

from . import db

log = logging.getLogger("delivery")

INSTANCE_ID = f"{os.environ.get('INSTANCE_ID') or socket.gethostname()}-{uuid.uuid4().hex[:6]}"
CDC_URL = os.environ.get("CDC_URL", "http://127.0.0.1:8002").rstrip("/")
RESEARCH_URL = os.environ.get("RESEARCH_URL", "http://127.0.0.1:8003").rstrip("/")
TARGET_URLS = {"cdc": CDC_URL, "research": RESEARCH_URL}
TARGETS = tuple(TARGET_URLS)
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "3"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.5"))
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", "15"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS corrections (
  id         TEXT PRIMARY KEY,
  station_id TEXT NOT NULL,
  day        TEXT NOT NULL,
  value      INTEGER NOT NULL CHECK (value >= 0),
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,
  id              TEXT NOT NULL UNIQUE,
  correction_id   TEXT NOT NULL REFERENCES corrections(id),
  target          TEXT NOT NULL CHECK (target IN ('cdc','research')),
  station_id      TEXT NOT NULL,
  day             TEXT NOT NULL,
  value           INTEGER NOT NULL CHECK (value >= 0),
  status          TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','inflight','confirmed')),
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at REAL NOT NULL DEFAULT 0,
  last_error      TEXT,
  receipt         TEXT,
  created_at      REAL NOT NULL,
  confirmed_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_orders_channel ON orders(target, station_id, status, seq);
CREATE TABLE IF NOT EXISTS channels (
  id            TEXT PRIMARY KEY,           -- '<target>|<station_id>'
  target        TEXT NOT NULL,
  station_id    TEXT NOT NULL,
  lease_owner   TEXT,
  lease_expires REAL
);
CREATE TABLE IF NOT EXISTS current_values (
  station_id TEXT NOT NULL,
  day        TEXT NOT NULL,
  value      INTEGER NOT NULL,
  updated_at REAL NOT NULL,
  PRIMARY KEY (station_id, day)
);
"""


def init_schema() -> None:
    db.init_schema(SCHEMA)


def accept_correction(station_id: str, day: str, value: int) -> str:
    """Persist the new current value and one order per target DB, atomically.

    Returns the correction id. Acceptance is serialized by the DB write lock,
    so the stored current value always moves forward with the latest accepted
    correction and never regresses.
    """
    cid = uuid.uuid4().hex
    now = time.time()
    with db.tx() as c:
        c.execute(
            "INSERT INTO corrections(id, station_id, day, value, created_at) VALUES (?,?,?,?,?)",
            (cid, station_id, day, value, now),
        )
        c.execute(
            """INSERT INTO current_values(station_id, day, value, updated_at) VALUES (?,?,?,?)
               ON CONFLICT(station_id, day) DO UPDATE
               SET value = excluded.value, updated_at = excluded.updated_at""",
            (station_id, day, value, now),
        )
        for target in TARGETS:
            c.execute(
                """INSERT INTO orders(id, correction_id, target, station_id, day, value, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (uuid.uuid4().hex, cid, target, station_id, day, value, now),
            )
            c.execute(
                "INSERT OR IGNORE INTO channels(id, target, station_id) VALUES (?,?,?)",
                (f"{target}|{station_id}", target, station_id),
            )
    return cid


class Dispatcher:
    """Background delivery engine (one per delivery instance)."""

    def __init__(self) -> None:
        self._workers: dict[str, asyncio.Task] = {}
        self._client: httpx.AsyncClient | None = None
        self._loop_task: asyncio.Task | None = None
        self._stopped = False

    async def start(self) -> None:
        # Crash recovery: orders left 'inflight' by a dead instance were never
        # confirmed; requeue them. Receivers dedupe by order id, so resending
        # an order the receiver already committed is still applied once.
        with db.tx() as c:
            c.execute("UPDATE orders SET status='pending' WHERE status='inflight'")
        self._client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        self._stopped = False
        self._loop_task = asyncio.create_task(self._run())
        log.info("dispatcher started (instance=%s)", INSTANCE_ID)

    async def stop(self) -> None:
        self._stopped = True
        if self._loop_task:
            self._loop_task.cancel()
        for t in self._workers.values():
            t.cancel()
        if self._client:
            await self._client.aclose()

    async def _run(self) -> None:
        while not self._stopped:
            try:
                for ch in self._claim_channels():
                    t = self._workers.get(ch)
                    if t is None or t.done():
                        self._workers[ch] = asyncio.create_task(self._channel_worker(ch))
            except Exception:
                log.exception("dispatch iteration failed")
            await asyncio.sleep(POLL_INTERVAL)

    def _claim_channels(self) -> list[str]:
        """Atomically lease channels that have due unconfirmed orders."""
        now = time.time()
        rows = db.query(
            """SELECT c.id AS id FROM channels c
               WHERE (c.lease_expires IS NULL OR c.lease_expires < ?)
                 AND EXISTS (SELECT 1 FROM orders o
                             WHERE o.target = c.target AND o.station_id = c.station_id
                               AND o.status <> 'confirmed' AND o.next_attempt_at <= ?)""",
            (now, now),
        )
        claimed = []
        for r in rows:
            with db.tx() as c:
                cur = c.execute(
                    """UPDATE channels SET lease_owner = ?, lease_expires = ?
                       WHERE id = ? AND (lease_expires IS NULL OR lease_expires < ?)""",
                    (INSTANCE_ID, now + LEASE_SECONDS, r["id"], now),
                )
                if cur.rowcount == 1:
                    claimed.append(r["id"])
        if claimed:
            log.info("claimed channels: %s", claimed)
        return claimed

    def _renew(self, channel_id: str) -> None:
        db.execute(
            "UPDATE channels SET lease_expires = ? WHERE id = ? AND lease_owner = ?",
            (time.time() + LEASE_SECONDS, channel_id, INSTANCE_ID),
        )

    def _release(self, channel_id: str) -> None:
        db.execute(
            "UPDATE channels SET lease_owner = NULL, lease_expires = NULL WHERE id = ? AND lease_owner = ?",
            (channel_id, INSTANCE_ID),
        )

    async def _channel_worker(self, channel_id: str) -> None:
        target, station_id = channel_id.split("|", 1)
        base = TARGET_URLS[target]
        try:
            while not self._stopped:
                order = db.query_one(
                    """SELECT * FROM orders
                       WHERE target = ? AND station_id = ? AND status <> 'confirmed'
                       ORDER BY seq LIMIT 1""",
                    (target, station_id),
                )
                if order is None:
                    return  # channel drained
                now = time.time()
                if order["next_attempt_at"] > now:
                    self._renew(channel_id)
                    await asyncio.sleep(min(order["next_attempt_at"] - now, 2.0))
                    continue
                # Persist the attempt before sending, so a crash mid-flight is
                # visible and retry backoff survives restarts.
                with db.tx() as c:
                    c.execute(
                        "UPDATE orders SET status='inflight', attempts=attempts+1 WHERE id=?",
                        (order["id"],),
                    )
                self._renew(channel_id)
                payload = {
                    "order_id": order["id"],
                    "correction_id": order["correction_id"],
                    "station_id": order["station_id"],
                    "day": order["day"],
                    "value": order["value"],
                }
                try:
                    resp = await self._client.post(base + "/apply", json=payload)
                    try:
                        body = resp.json()
                    except Exception:
                        body = {}
                    if (
                        resp.status_code == 200
                        and body.get("order_id") == order["id"]
                        and body.get("status") == "applied"
                    ):
                        with db.tx() as c:
                            c.execute(
                                """UPDATE orders SET status='confirmed', confirmed_at=?,
                                   receipt=?, last_error=NULL WHERE id=?""",
                                (time.time(), json.dumps(body), order["id"]),
                            )
                    else:
                        self._schedule_retry(
                            order, f"receiver responded {resp.status_code}: {resp.text[:200]}"
                        )
                except Exception as exc:  # timeout / connection error / ...
                    self._schedule_retry(order, f"{type(exc).__name__}: {exc}")
        finally:
            self._release(channel_id)

    def _schedule_retry(self, order, err: str) -> None:
        attempts = order["attempts"] + 1
        backoff = min(0.5 * (2 ** min(attempts, 5)), 15.0)
        log.warning("order %s failed (%s); retry in %.1fs", order["id"], err, backoff)
        with db.tx() as c:
            c.execute(
                "UPDATE orders SET status='pending', next_attempt_at=?, last_error=? WHERE id=?",
                (time.time() + backoff, err[:500], order["id"]),
            )
