#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${ROOT}/.local-run"
LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"
ENV_FILE="${ROOT}/.env.local"
FALLBACK_ENV_FILE="${ROOT}/.env"

KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-alpaca-bars}"
ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI:-sqlite:///./warehouse/catalog.db}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-./warehouse}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
BATCH_INTERVAL="${BATCH_INTERVAL:-300}"
METRICS_INTERVAL="${METRICS_INTERVAL:-5}"
ALPACA_SYMBOLS="${ALPACA_SYMBOLS:-AAPL,TSLA,NVDA}"
LOADER_PORT="${LOADER_PORT:-8081}"
PORT="${PORT:-8080}"

load_env_file() {
  if [[ -f "${FALLBACK_ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${FALLBACK_ENV_FILE}"
    set +a
  fi
  if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  fi
}

ensure_dirs() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
}

pid_file() {
  printf "%s/%s.pid" "${PID_DIR}" "$1"
}

is_running() {
  local name="$1"
  local file
  file="$(pid_file "${name}")"
  [[ -f "${file}" ]] && kill -0 "$(cat "${file}")" 2>/dev/null
}

start_process() {
  local name="$1"
  shift

  ensure_dirs
  if is_running "${name}"; then
    echo "${name} already running (pid $(cat "$(pid_file "${name}")"))"
    return 0
  fi

  echo "starting ${name}; log: ${LOG_DIR}/${name}.log"
  if command -v setsid >/dev/null 2>&1; then
    (
      cd "${ROOT}"
      nohup setsid "$@" >"${LOG_DIR}/${name}.log" 2>&1 < /dev/null &
      echo "$!" >"$(pid_file "${name}")"
    )
  else
    (
      cd "${ROOT}"
      nohup "$@" >"${LOG_DIR}/${name}.log" 2>&1 < /dev/null &
      echo "$!" >"$(pid_file "${name}")"
    )
  fi
}

stop_process() {
  local name="$1"
  local file
  file="$(pid_file "${name}")"

  if ! [[ -f "${file}" ]]; then
    echo "${name} not running"
    return 0
  fi

  local pid
  pid="$(cat "${file}")"
  if kill -0 "${pid}" 2>/dev/null; then
    echo "stopping ${name} (pid ${pid})"
    kill "${pid}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "force stopping ${name} (pid ${pid})"
      kill -9 "${pid}" 2>/dev/null || true
    fi
  else
    echo "${name} pid ${pid} is not running"
  fi
  rm -f "${file}"
}

status_process() {
  local name="$1"
  if is_running "${name}"; then
    echo "${name}: running (pid $(cat "$(pid_file "${name}")"))"
  else
    echo "${name}: stopped"
  fi
}

require_alpaca_creds() {
  if [[ -z "${ALPACA_KEY:-}" || -z "${ALPACA_SECRET:-}" ]]; then
    cat >&2 <<EOF
Missing ALPACA_KEY / ALPACA_SECRET.

Set them in your shell, ${FALLBACK_ENV_FILE}, or ${ENV_FILE}:
ALPACA_KEY=...
ALPACA_SECRET=...
ALPACA_SYMBOLS=AAPL,TSLA,NVDA
EOF
    exit 1
  fi
}

start_all() {
  load_env_file
  require_alpaca_creds
  ensure_dirs

  (cd "${ROOT}" && make up)
  (cd "${ROOT}" && uv run --package load python scripts/wait_kafka.py --broker "${KAFKA_BROKER}")

  start_process loader env \
    KAFKA_BROKER="${KAFKA_BROKER}" \
    KAFKA_TOPIC="${KAFKA_TOPIC}" \
    ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI}" \
    ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE}" \
    LOG_MODE=stdout \
    PORT="${LOADER_PORT}" \
    BATCH_SIZE="${BATCH_SIZE}" \
    BATCH_INTERVAL="${BATCH_INTERVAL}" \
    METRICS_INTERVAL="${METRICS_INTERVAL}" \
    uv run --package load python load/subscriber.py

  start_process extractor env \
    ALPACA_KEY="${ALPACA_KEY}" \
    ALPACA_SECRET="${ALPACA_SECRET}" \
    ALPACA_WS_URI="${ALPACA_WS_URI:-wss://stream.data.alpaca.markets/v2/iex}" \
    ALPACA_SYMBOLS="${ALPACA_SYMBOLS}" \
    KAFKA_BROKER="${KAFKA_BROKER}" \
    KAFKA_TOPIC="${KAFKA_TOPIC}" \
    METRICS_INTERVAL="${METRICS_INTERVAL}" \
    DATA_IDLE_TIMEOUT="${DATA_IDLE_TIMEOUT:-600}" \
    bash -lc "cd wsr && cargo run --release"

  start_process frontend-api env \
    ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI}" \
    ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE}" \
    PORT="${PORT}" \
    uv run --package frontend python frontend/server.py

  if [[ -d "${ROOT}/frontend/web/node_modules" ]]; then
    start_process frontend-web bash -lc "cd frontend/web && npm run dev -- --host 0.0.0.0"
  else
    echo "frontend-web not started: run '(cd frontend/web && npm install)' first"
  fi

  status_all
  echo "API: http://localhost:${PORT}"
  echo "UI : http://localhost:5173"
  echo "Loader health: http://localhost:${LOADER_PORT}"
}

stop_all() {
  stop_process frontend-web
  stop_process frontend-api
  stop_process extractor
  stop_process loader
  (cd "${ROOT}" && make down)
}

status_all() {
  status_process loader
  status_process extractor
  status_process frontend-api
  status_process frontend-web
}

tail_logs() {
  ensure_dirs
  if command -v tail >/dev/null 2>&1; then
    tail -n 80 -f "${LOG_DIR}"/*.log
  fi
}

case "${1:-}" in
  up)
    start_all
    ;;
  down)
    stop_all
    ;;
  status)
    status_all
    ;;
  logs)
    tail_logs
    ;;
  *)
    cat <<EOF
Usage: $0 up|down|status|logs

Optional ${FALLBACK_ENV_FILE} or ${ENV_FILE}:
ALPACA_KEY=...
ALPACA_SECRET=...
ALPACA_SYMBOLS=AAPL,TSLA,NVDA
KAFKA_TOPIC=alpaca-bars
BATCH_SIZE=10
BATCH_INTERVAL=5
LOADER_PORT=8081
EOF
    exit 2
    ;;
esac
