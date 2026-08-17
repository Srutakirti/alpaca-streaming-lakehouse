#!/usr/bin/env bash
# Start the Rust extractor against Alpaca's credentialed FAKEPACA test stream.
set -euo pipefail

local_extractor_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$local_extractor_root/scripts/local_fakepaca_env.sh"

for credential_file in "$local_extractor_root/.env" "$local_extractor_root/.env.local"; do
  if [[ -f "$credential_file" ]]; then
    set -a
    source "$credential_file"
    set +a
  fi
done

missing=()
[[ -n "${ALPACA_KEY:-}" ]] || missing+=(ALPACA_KEY)
[[ -n "${ALPACA_SECRET:-}" ]] || missing+=(ALPACA_SECRET)
if (( ${#missing[@]} )); then
  echo "missing ${missing[*]}; define them in .env or .env.local" >&2
  exit 1
fi

image="${WSR_IMAGE:-gce-hadoop-catalog-websocket-extractor:local}"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "build the extractor first: docker build -t $image websocket-extractor-rust" >&2
  exit 1
fi

exec docker run --rm --network host \
  --name "gce-hadoop-catalog-fakepaca-${KAFKA_BROKER##*:}" \
  -e ALPACA_KEY -e ALPACA_SECRET -e KAFKA_BROKER -e KAFKA_TOPIC \
  -e ALPACA_WS_URI=wss://stream.data.alpaca.markets/v2/test \
  -e ALPACA_SYMBOLS=FAKEPACA \
  -e DATA_IDLE_TIMEOUT=0 \
  "$image"
