#!/usr/bin/env bash
set -euo pipefail

native_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec java -cp "$native_root/lib/iceberg-loader.jar${GCS_CONNECTOR_JAR:+:$GCS_CONNECTOR_JAR}" \
  io.gcehcatalog.loader.TopicProvisioner
