#!/usr/bin/env bash
set -euo pipefail
native_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec java -cp "$native_root/lib/iceberg-loader.jar" io.gcehcatalog.loader.SyntheticPublisher
