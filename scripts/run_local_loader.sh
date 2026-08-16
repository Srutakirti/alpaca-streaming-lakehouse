#!/usr/bin/env bash
set -euo pipefail

runtime_dir="${PIPELINE_RUNTIME_DIR:-.local-run}"
mkdir -p "$runtime_dir"

# The production topology has one VM writer. This advisory OS lock prevents a
# second local process (manual launch, restart, or deploy overlap) from writing.
exec flock -n "$runtime_dir/loader.lock" \
  java -jar iceberg-loader-java/target/iceberg-loader-0.1.0.jar
