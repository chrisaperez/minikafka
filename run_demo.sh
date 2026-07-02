#!/usr/bin/env bash
# Runs the full chaos demo: broker + lag monitor + producer + 3 consumers
# that crash on purpose. Dashboard: http://127.0.0.1:9600
set -euo pipefail
cd "$(dirname "$0")"

BROKER_PORT="${BROKER_PORT:-9092}"
MONITOR_PORT="${MONITOR_PORT:-9600}"
TOTAL="${TOTAL:-1000000}"
RATE="${RATE:-4000}"
THRESHOLD="${THRESHOLD:-5000}"

pids=()
cleanup() {
  echo; echo "[demo] shutting down..."
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

python3 -m minikafka.server --port "$BROKER_PORT" --data-dir ./data &
pids+=($!)
sleep 1

python3 chaos/monitor/server.py \
  --broker "http://127.0.0.1:$BROKER_PORT" \
  --group chaos --port "$MONITOR_PORT" --threshold "$THRESHOLD" &
pids+=($!)

python3 -m chaos.supervisor \
  --broker "http://127.0.0.1:$BROKER_PORT" \
  --consumers 3 --rate "$RATE" --total "$TOTAL" \
  --crash-mean-s 30 --restart-delay 6 &
pids+=($!)

echo
echo "[demo] dashboard: http://127.0.0.1:$MONITOR_PORT"
echo "[demo] Ctrl-C to stop everything"
wait
