#!/usr/bin/env bash
# Build and push both Docker images to Artifact Registry.
# Run from the workspace root before terraform apply.
#
# Usage: bash scripts/build_and_push.sh <tag>
# Example: bash scripts/build_and_push.sh v0.1.0

set -euo pipefail

TAG="${1:?Usage: $0 <tag>}"
PROJECT="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-east1}"
REPO="${REPO:-alpaca-datalake}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}"

echo "Building and pushing to ${REGISTRY} with tag ${TAG}"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "==> Building alpaca-extractor"
docker build -t "${REGISTRY}/alpaca-extractor:${TAG}" -f extract/Dockerfile .

echo "==> Building alpaca-loader"
docker build -t "${REGISTRY}/alpaca-loader:${TAG}" -f load/Dockerfile .

echo "==> Pushing alpaca-extractor"
docker push "${REGISTRY}/alpaca-extractor:${TAG}"

echo "==> Pushing alpaca-loader"
docker push "${REGISTRY}/alpaca-loader:${TAG}"

echo ""
echo "Done. Update terraform.tfvars:"
echo "  image_tag = \"${TAG}\""
echo "Then run: terraform -chdir=terraform apply -var image_tag=${TAG}"
