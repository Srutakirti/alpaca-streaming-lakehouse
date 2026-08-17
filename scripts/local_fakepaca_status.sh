#!/usr/bin/env bash
# Report whether the three live pipeline components are currently running.
set -euo pipefail

local_status_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$local_status_root/scripts/local_fakepaca_env.sh"

broker_port="${KAFKA_BROKER##*:}"
docker ps --filter "name=gce-hadoop-catalog-tansu-$broker_port" \
  --filter "name=gce-hadoop-catalog-fakepaca-$broker_port" \
  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
loader_processes="$(pgrep -af 'iceberg-loader-java/target/iceberg-loader-0.1.0.jar' || true)"
if [[ -n "$loader_processes" ]]; then
  printf 'loader_processes:\n%s\n' "$loader_processes"
else
  printf 'loader_processes: not running\n'
fi
printf 'warehouse=%s\ntransport=%s\nloader_batch_records=%s\nloader_batch_seconds=%s\n' \
  "$ICEBERG_WAREHOUSE" "$LOCAL_FAKEPACA_TRANSPORT_DIR" "$LOADER_MAX_RECORDS" "$LOADER_MAX_SECONDS"
