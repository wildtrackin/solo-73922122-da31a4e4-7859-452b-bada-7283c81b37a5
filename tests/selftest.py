#!/usr/bin/env python3
"""端到端自测：针对运行中的部署（docker compose 或 scripts/dev_local.sh）。

环境变量：
  DELIVERY1 / DELIVERY2   投递实例地址（默认 http://127.0.0.1:8000 / :8001）
  CDC / RESEARCH          接收端地址（默认 :8002 / :8003）
  RESTART_CMD             可选；使 delivery-1 退出并被拉起的命令（自测会验证实例 ID 变化）。
                          compose: "curl -sf -X POST http://127.0.0.1:8000/admin/crash"
                          （delivery-1 需设 CHAOS_CRASH=1，由 restart 策略/守护循环拉起）
退出码非 0 表示有用例失败。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

import httpx

D1 = os.environ.get("DELIVERY1", "http://127.0.0.1:8000")
D2 = os.environ.get("DELIVERY2", "http://127.0.0.1:8001")
CDC = os.environ.get("CDC", "http://127.0.0.1:8002")
RES = os.environ.get("RESEARCH", "http://127.0.0.1:8003")
RESTART_CMD = os.environ.get("RESTART_CMD")
DAY = "2026-11-16"

client = httpx.Client(timeout=10)
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def wait_until(pred, timeout=30.0, interval=0.25) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def submit(base: str, station: str, day: str, value: int) -> str:
    r = client.post(base + "/corrections", json={"station_id": station, "day": day, "value": value})
    assert r.status_code == 202, r.text
    return r.json()["correction"]["id"]


def orders_of(base: str, cid: str) -> list[dict]:
    return client.get(base + f"/corrections/{cid}").json()["orders"]


def order_for(base: str, cid: str, target: str) -> dict:
    for o in orders_of(base, cid):
        if o["target"] == target:
            return o
    raise KeyError(target)


def confirmed(base: str, cid: str, target: str) -> bool:
    return order_for(base, cid, target)["status"] == "confirmed"


def recv_current(base: str, station: str, day: str):
    return client.get(base + f"/current/{station}").json()["values"].get(day)


def applications(base: str, station: str | None = None) -> list[dict]:
    rows = client.get(base + "/applications").json()["applications"]
    return [a for a in rows if station is None or a["station_id"] == station]


# ---------------- test cases ----------------

def t0_health():
    for name, base in [("delivery-1", D1), ("delivery-2", D2), ("cdc", CDC), ("research", RES)]:
        try:
            ok = client.get(base + "/health").status_code == 200
        except Exception:
            ok = False
        check(f"health {name}", ok)


def t1_validation():
    bad = [
        {"station_id": "S", "day": DAY, "value": -1},          # 负数
        {"station_id": "S", "day": "2026-13-40", "value": 1},  # 非法日期
        {"station_id": "S", "day": DAY, "value": 1.5},         # 非整数
        {"station_id": "", "day": DAY, "value": 1},            # 空站号
        {"station_id": "S", "day": DAY},                       # 缺字段
    ]
    codes = [client.post(D1 + "/corrections", json=b).status_code for b in bad]
    check("validation: 非法输入全部 422", all(c == 422 for c in codes), str(codes))


def t2_basic_flow():
    cid = submit(D1, "ST-A", DAY, 100)
    ok = wait_until(lambda: confirmed(D2, cid, "cdc") and confirmed(D2, cid, "research"), 20)
    check("basic: 两库均 confirmed（经另一实例查询）", ok)
    cur = client.get(D1 + "/current").json()["current"]
    check("basic: 投递服务现值=100",
          any(r["station_id"] == "ST-A" and r["day"] == DAY and r["value"] == 100 for r in cur))
    check("basic: 疾控库现值=100", recv_current(CDC, "ST-A", DAY) == 100)
    check("basic: 科研库现值=100", recv_current(RES, "ST-A", DAY) == 100)
    o = order_for(D1, cid, "cdc")
    apps = [a for a in applications(CDC, "ST-A") if a["order_id"] == o["id"]]
    check("basic: 疾控库恰好一条应用记录", len(apps) == 1)


def t3_slow_station_does_not_block():
    client.post(CDC + "/fault", json={"station_id": "ST-B", "delay_ms": 2500})
    cid_b = submit(D1, "ST-B", DAY, 1)
    cid_c = submit(D1, "ST-C", DAY, 2)
    time.sleep(1.2)
    st_b = order_for(D1, cid_b, "cdc")["status"]
    check("slow: 1.2s 时 ST-B 在疾控库仍未确认（未虚报送达）", st_b != "confirmed", st_b)
    check("slow: 同库其他站 ST-C 不受影响", confirmed(D1, cid_c, "cdc"))
    check("slow: 另一库（科研）两站均已确认",
          confirmed(D1, cid_b, "research") and confirmed(D1, cid_c, "research"))
    ok = wait_until(lambda: confirmed(D1, cid_b, "cdc"), 15)
    check("slow: ST-B 延迟结束后在疾控库确认", ok)
    client.delete(CDC + "/fault")


def t4_lost_receipt_applied_once():
    client.post(RES + "/fault", json={"station_id": "ST-D", "mode": "drop_after_commit", "times": 1})
    cid = submit(D1, "ST-D", DAY, 7)
    ok = wait_until(lambda: confirmed(D1, cid, "research"), 20)
    check("lost-receipt: 重发后最终确认", ok)
    o = order_for(D1, cid, "research")
    apps = [a for a in applications(RES, "ST-D") if a["order_id"] == o["id"]]
    check("lost-receipt: 只应用一次", len(apps) == 1,
          f"applications={len(apps)} attempts={o['attempts']}")
    check("lost-receipt: 重发收到 duplicate=true 回执",
          bool(o.get("receipt")) and '"duplicate": true' in o["receipt"])
    check("lost-receipt: 科研库现值=7", recv_current(RES, "ST-D", DAY) == 7)


def t5_receiver_failing():
    client.post(CDC + "/fault", json={"station_id": "ST-E", "mode": "fail"})
    cid_e = submit(D1, "ST-E", DAY, 9)
    cid_f = submit(D1, "ST-F", DAY, 10)
    time.sleep(2.0)
    o_e = order_for(D1, cid_e, "cdc")
    check("unreachable: 故障中 ST-E 未确认且已重试",
          o_e["status"] != "confirmed" and o_e["attempts"] >= 1,
          f"status={o_e['status']} attempts={o_e['attempts']}")
    check("unreachable: 其他站 ST-F 正常确认", confirmed(D1, cid_f, "cdc"))
    check("unreachable: ST-E 在科研库正常确认", confirmed(D1, cid_e, "research"))
    client.delete(CDC + "/fault")
    ok = wait_until(lambda: confirmed(D1, cid_e, "cdc"), 20)
    check("unreachable: 恢复后 ST-E 确认", ok)
    apps = [a for a in applications(CDC, "ST-E") if a["order_id"] == o_e["id"]]
    check("unreachable: 恢复后只应用一次", len(apps) == 1)


def t6_burst_dual_instance():
    stations = ["ST-G", "ST-H", "ST-I", "ST-J"]
    plan = []
    for i in range(24):
        st = stations[i % len(stations)]
        base = D1 if i % 2 == 0 else D2  # 交替打到两个实例
        plan.append((submit(base, st, DAY, 1000 + i), st, 1000 + i))
    ok = wait_until(
        lambda: all(confirmed(D1, c, "cdc") and confirmed(D1, c, "research") for c, _, _ in plan),
        60,
    )
    check("burst: 24 笔更正在两库全部确认", ok)
    for base, name in ((CDC, "cdc"), (RES, "research")):
        ids = [a["order_id"] for a in applications(base)]
        check(f"burst: {name} 无重复应用", len(ids) == len(set(ids)),
              f"{len(ids) - len(set(ids))} 个重复")
    last = {}
    for _, st, v in plan:
        last[st] = v
    for st, v in last.items():
        check(f"burst: {st} 终值={v}（两库一致且不倒退）",
              recv_current(CDC, st, DAY) == v and recv_current(RES, st, DAY) == v)


def t7_per_station_order():
    n = 12
    cids = [submit(D1, "ST-K", DAY, i + 1) for i in range(n)]
    ok = wait_until(lambda: all(confirmed(D1, c, "cdc") and confirmed(D1, c, "research") for c in cids), 60)
    check("order: 全部确认", ok)
    for base, name in ((CDC, "cdc"), (RES, "research")):
        vals = [a["value"] for a in applications(base, "ST-K")]
        check(f"order: {name} 按受理顺序应用", vals == list(range(1, n + 1)), str(vals))


def t8_restart_recovery():
    if not RESTART_CMD:
        print("[SKIP] restart recovery（未设置 RESTART_CMD）")
        return
    inst_before = client.get(D1 + "/health").json()["instance"]
    client.post(CDC + "/fault", json={"station_id": "ST-L", "delay_ms": 700})
    cids = [submit(D1, "ST-L", DAY, 2000 + i) for i in range(10)]
    time.sleep(0.8)  # 让部分单在途
    subprocess.run(RESTART_CMD, shell=True, check=False)
    restarted = wait_until(
        lambda: client.get(D1 + "/health").json()["instance"] != inst_before, 30
    )
    check("restart: delivery-1 确已退出并以新实例恢复", restarted)
    ok = wait_until(
        lambda: all(confirmed(D2, c, "cdc") and confirmed(D2, c, "research") for c in cids), 90
    )
    check("restart: 实例重启后积压单全部续送完成", ok)
    client.delete(CDC + "/fault")
    ids = [a["order_id"] for a in applications(CDC, "ST-L")]
    check("restart: 不漏单不重复", len(ids) == len(set(ids)) == 10,
          f"{len(ids)} 条应用 / {len(set(ids))} 个唯一单")
    check("restart: 终值=最后一笔（不倒退）", recv_current(CDC, "ST-L", DAY) == 2009)


def main() -> None:
    t0_health()
    client.delete(CDC + "/fault")
    client.delete(RES + "/fault")
    t1_validation()
    t2_basic_flow()
    t3_slow_station_does_not_block()
    t4_lost_receipt_applied_once()
    t5_receiver_failing()
    t6_burst_dual_instance()
    t7_per_station_order()
    t8_restart_recovery()
    print()
    if failures:
        print(f"{len(failures)} 项失败: {failures}")
        sys.exit(1)
    print("全部自测通过")


if __name__ == "__main__":
    main()
