#!/usr/bin/env bash
# Start the long-lived, single-writer Java loader in the foreground.
set -euo pipefail

local_loader_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$local_loader_root/scripts/local_fakepaca_env.sh"

if [[ ! -f "$local_loader_root/iceberg-loader-java/target/iceberg-loader-0.1.0.jar" ]]; then
  echo "build the loader first: mvn -f iceberg-loader-java/pom.xml package" >&2
  exit 1
fi

exec "$local_loader_root/scripts/run_local_loader.sh"
