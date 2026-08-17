#!/usr/bin/env bash
# Source this file from each terminal before starting a live local component.
# It deliberately keeps Tansu transport state in /tmp and the Iceberg warehouse
# in .local-notebook, so a broker restart cannot delete the explored catalog.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "source scripts/local_fakepaca_env.sh; do not execute it directly" >&2
  exit 1
fi

set -euo pipefail

local_fakepaca_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LOCAL_FAKEPACA_TRANSPORT_DIR="${LOCAL_FAKEPACA_TRANSPORT_DIR:-/tmp/gce-hadoop-catalog-fakepaca-$(id -u)}"
export LOCAL_FAKEPACA_WAREHOUSE_DIR="${LOCAL_FAKEPACA_WAREHOUSE_DIR:-$local_fakepaca_root/.local-notebook/warehouse}"
export PIPELINE_RUNTIME_DIR="$LOCAL_FAKEPACA_TRANSPORT_DIR"
export KAFKA_BROKER="${KAFKA_BROKER:-127.0.0.1:19092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-alpaca-bars}"
export KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-fakepaca-live}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-file://$LOCAL_FAKEPACA_WAREHOUSE_DIR}"
export LOADER_MAX_RECORDS="${LOADER_MAX_RECORDS:-100}"
export LOADER_MAX_SECONDS="${LOADER_MAX_SECONDS:-300}"

mkdir -p "$LOCAL_FAKEPACA_TRANSPORT_DIR/tansu" "$LOCAL_FAKEPACA_WAREHOUSE_DIR"
