#!/usr/bin/env bash
# Start the local Tansu broker in the foreground. Stop it with Ctrl-C.
set -euo pipefail

local_tansu_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$local_tansu_root/scripts/local_fakepaca_env.sh"

if ! command -v docker >/dev/null; then
  echo "docker is required to start Tansu" >&2
  exit 1
fi

broker_port="${KAFKA_BROKER##*:}"
exec docker run --rm \
  --name "gce-hadoop-catalog-tansu-$broker_port" \
  --pull=never \
  --network host \
  --volume "$LOCAL_FAKEPACA_TRANSPORT_DIR/tansu:/var/lib/tansu" \
  "${TANSU_IMAGE:-ghcr.io/tansu-io/tansu:0.6.0}" \
  --kafka-listener-url "tcp://127.0.0.1:$broker_port" \
  --kafka-advertised-listener-url "tcp://127.0.0.1:$broker_port" \
  --storage-engine sqlite:///var/lib/tansu/tansu.sqlite
