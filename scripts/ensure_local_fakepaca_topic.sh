#!/usr/bin/env bash
# Create the topic once after Tansu is listening. Safe to invoke repeatedly.
set -euo pipefail

local_topic_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$local_topic_root/scripts/local_fakepaca_env.sh"

exec uv run python -c '
from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.runtime import ensure_topic
ensure_topic(LocalSettings.from_environment())
print("topic_ready=" + LocalSettings.from_environment().topic)
'
