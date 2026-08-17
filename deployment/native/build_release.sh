#!/usr/bin/env bash
# Build a Docker-free x86_64 Linux release bundle outside the target VM.
set -euo pipefail

release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
release_id="${RELEASE_ID:-$(git -C "$release_root" rev-parse --short HEAD)}"
release_dir="${RELEASE_DIR:-$release_root/dist/gce-hadoop-catalog-$release_id}"
gcs_connector_version="${GCS_CONNECTOR_VERSION:-hadoop3-2.2.30}"
gcs_connector_url="https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$gcs_connector_version/gcs-connector-$gcs_connector_version-shaded.jar"

if [[ -z "${TANSU_BINARY:-}" || ! -x "${TANSU_BINARY:-}" ]]; then
  echo "set TANSU_BINARY to the verified pinned Tansu 0.6.0 Linux x86_64 executable" >&2
  exit 1
fi

if [[ -e "$release_dir" || -e "$release_dir.tar.gz" ]]; then
  echo "release output already exists: $release_dir (choose another RELEASE_ID or RELEASE_DIR)" >&2
  exit 1
fi
mkdir -p "$release_dir/bin" "$release_dir/lib" "$release_dir/systemd" "$release_dir/scripts"

mvn -f "$release_root/iceberg-loader-java/pom.xml" package
cargo build --manifest-path "$release_root/websocket-extractor-rust/Cargo.toml" --release --locked

cp "$TANSU_BINARY" "$release_dir/bin/tansu"
cp "$release_root/websocket-extractor-rust/target/release/wsr" "$release_dir/bin/wsr"
cp "$release_root/iceberg-loader-java/target/iceberg-loader-0.1.0.jar" "$release_dir/lib/iceberg-loader.jar"
curl --fail --location --silent --show-error "$gcs_connector_url" -o "$release_dir/lib/gcs-connector.jar"
cp "$release_root/deployment/native/ensure-topic.sh" "$release_dir/bin/ensure-topic"
cp "$release_root/deployment/native/install_release.sh" "$release_dir/bin/install-release"
cp "$release_root/deployment/native/systemd/"*.service "$release_dir/systemd/"
cp "$release_root/scripts/run_local_loader.sh" "$release_dir/scripts/run_loader.sh"
chmod 0755 "$release_dir/bin/"* "$release_dir/scripts/run_loader.sh"

(
  cd "$release_dir"
  sha256sum bin/* lib/* scripts/* systemd/* > SHA256SUMS
)
tar -C "$(dirname "$release_dir")" -czf "$release_dir.tar.gz" "$(basename "$release_dir")"
printf 'release_bundle=%s.tar.gz\n' "$release_dir"
