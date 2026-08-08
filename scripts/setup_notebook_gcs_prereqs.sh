#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-project-66783f65-9c3e-4880-9a3}"
SA_NAME="${SA_NAME:-alpaca-spark-gcs-reader}"
SA_EMAIL="${SA_EMAIL:-${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
GCS_SERVICE_ACCOUNT_JSON="${GCS_SERVICE_ACCOUNT_JSON:-/tmp/alpaca-spark-gcs-reader.json}"
GCS_CONNECTOR_JAR="${GCS_CONNECTOR_JAR:-/tmp/gcs-connector-hadoop3-2.2.30-shaded.jar}"
GCS_CONNECTOR_URL="${GCS_CONNECTOR_URL:-https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.30/gcs-connector-hadoop3-2.2.30-shaded.jar}"

FORCE_KEY=false
FORCE_JAR=false

usage() {
  cat <<USAGE
Usage: $0 [--force-key] [--force-jar]

Checks and prepares local notebook prerequisites:
  - GCS service account key: ${GCS_SERVICE_ACCOUNT_JSON}
  - Spark GCS connector jar : ${GCS_CONNECTOR_JAR}

Environment overrides:
  PROJECT_ID
  SA_NAME
  SA_EMAIL
  GCS_SERVICE_ACCOUNT_JSON
  GCS_CONNECTOR_JAR
  GCS_CONNECTOR_URL
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force-key)
      FORCE_KEY=true
      shift
      ;;
    --force-jar)
      FORCE_JAR=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "Missing required command: ${name}" >&2
    exit 1
  fi
}

require_command gcloud
require_command curl

if gcloud iam service-accounts describe "${SA_EMAIL}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "OK service account exists: ${SA_EMAIL}"
else
  echo "Missing service account: ${SA_EMAIL}" >&2
  echo "Create it first or see docs/runbooks/local-spark-gcs-iceberg-notebook-setup.md" >&2
  exit 1
fi

if [[ -s "${GCS_SERVICE_ACCOUNT_JSON}" && "${FORCE_KEY}" == "false" ]]; then
  echo "OK GCS key exists: ${GCS_SERVICE_ACCOUNT_JSON}"
else
  if [[ -e "${GCS_SERVICE_ACCOUNT_JSON}" && "${FORCE_KEY}" == "true" ]]; then
    rm -f "${GCS_SERVICE_ACCOUNT_JSON}"
  fi

  echo "Creating GCS service account key: ${GCS_SERVICE_ACCOUNT_JSON}"
  mkdir -p "$(dirname "${GCS_SERVICE_ACCOUNT_JSON}")"
  gcloud iam service-accounts keys create "${GCS_SERVICE_ACCOUNT_JSON}" \
    --project="${PROJECT_ID}" \
    --iam-account="${SA_EMAIL}"
  chmod 600 "${GCS_SERVICE_ACCOUNT_JSON}"
fi

if [[ -s "${GCS_CONNECTOR_JAR}" && "${FORCE_JAR}" == "false" ]]; then
  echo "OK GCS connector jar exists: ${GCS_CONNECTOR_JAR}"
else
  if [[ -e "${GCS_CONNECTOR_JAR}" && "${FORCE_JAR}" == "true" ]]; then
    rm -f "${GCS_CONNECTOR_JAR}"
  fi

  echo "Downloading GCS connector jar: ${GCS_CONNECTOR_JAR}"
  mkdir -p "$(dirname "${GCS_CONNECTOR_JAR}")"
  curl -fL "${GCS_CONNECTOR_URL}" -o "${GCS_CONNECTOR_JAR}"
fi

echo
echo "Notebook prerequisites are ready."
echo "GCS_SERVICE_ACCOUNT_JSON=${GCS_SERVICE_ACCOUNT_JSON}"
echo "GCS_CONNECTOR_JAR=${GCS_CONNECTOR_JAR}"
