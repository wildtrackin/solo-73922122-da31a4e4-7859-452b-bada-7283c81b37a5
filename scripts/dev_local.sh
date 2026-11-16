#!/usr/bin/env bash
# 本地开发运行（无 Docker 时镜像 docker-compose.yml 的拓扑）：
#   2 个接收端(8002/8003，各自独立 SQLite) + 2 个投递实例(8000/8001，共享 SQLite)。
# 投递实例包在重启循环里，模拟 compose 的 restart 策略，便于做崩溃恢复测试。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-/tmp/wq-local}"
mkdir -p "$WORK"

if ! command -v uvicorn >/dev/null 2>&1; then
  echo "uvicorn 不在 PATH；请先准备含 fastapi/uvicorn/httpx 的环境（如 python -m venv）。" >&2
  exit 1
fi

pids=()
start() { setsid "$@" & pids+=($!); }
cleanup() {
  echo "stopping..."
  for p in "${pids[@]}"; do kill -- -"$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

start env NAME=cdc      DB_PATH="$WORK/cdc.db"      uvicorn --app-dir "$ROOT/receiver" app.main:app --port 8002 >"$WORK/cdc.log" 2>&1
start env NAME=research DB_PATH="$WORK/research.db" uvicorn --app-dir "$ROOT/receiver" app.main:app --port 8003 >"$WORK/research.log" 2>&1

DELIVERY_ENV="DB_PATH=$WORK/delivery.db CDC_URL=http://127.0.0.1:8002 RESEARCH_URL=http://127.0.0.1:8003"
start bash -c "while true; do env INSTANCE_ID=delivery-1 $DELIVERY_ENV uvicorn --app-dir '$ROOT/delivery' app.main:app --port 8000 >>'$WORK/delivery-1.log' 2>&1; sleep 1; done"
start bash -c "while true; do env INSTANCE_ID=delivery-2 $DELIVERY_ENV uvicorn --app-dir '$ROOT/delivery' app.main:app --port 8001 >>'$WORK/delivery-2.log' 2>&1; sleep 1; done"

echo "delivery-1:  http://127.0.0.1:8000/docs"
echo "delivery-2:  http://127.0.0.1:8001/docs"
echo "cdc:         http://127.0.0.1:8002/docs"
echo "research:    http://127.0.0.1:8003/docs"
echo "logs:        $WORK/*.log   (Ctrl+C 停止全部)"
wait
