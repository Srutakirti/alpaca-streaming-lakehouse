#!/usr/bin/env bash
set -euo pipefail

runtime_dir="${PIPELINE_RUNTIME_DIR:-.local-run}"
mkdir -p "$runtime_dir"
loader_jar="${LOADER_JAR:-iceberg-loader-java/target/iceberg-loader-0.1.0.jar}"
loader_classpath="$loader_jar"

if [[ -n "${GCS_CONNECTOR_JAR:-}" ]]; then
  if [[ ! -f "$GCS_CONNECTOR_JAR" ]]; then
    echo "GCS connector jar not found: $GCS_CONNECTOR_JAR" >&2
    exit 1
  fi
  loader_classpath="$loader_classpath:$GCS_CONNECTOR_JAR"
fi

# The production topology has one VM writer. This advisory OS lock prevents a
# second local process (manual launch, restart, or deploy overlap) from writing.
exec flock -n "$runtime_dir/loader.lock" \
  java -cp "$loader_classpath" io.gcehcatalog.loader.Main
