#!/usr/bin/env bash
# Build a Docker-free x86_64 Linux release bundle outside the target VM.
set -euo pipefail

release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
release_id="${RELEASE_ID:-$(git -C "$release_root" rev-parse --short HEAD)}"
release_dir="${RELEASE_DIR:-$release_root/dist/gce-hadoop-catalog-$release_id}"
gcs_connector_version="${GCS_CONNECTOR_VERSION:-hadoop3-2.2.30}"
gcs_connector_url="https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$gcs_connector_version/gcs-connector-$gcs_connector_version-shaded.jar"
tansu_version="0.6.0"
tansu_url="https://github.com/nisshi-io/nisshi/releases/download/v${tansu_version}/tansu-x86_64-unknown-linux-gnu.tar.gz"
tansu_sha256="e3f5ddb6be4f92fb082cae51c58a7745c7ec3e16a87bc4c984e4ddb5a552e1be"

if [[ -e "$release_dir" || -e "$release_dir.tar.gz" ]]; then
  echo "release output already exists: $release_dir (choose another RELEASE_ID or RELEASE_DIR)" >&2
  exit 1
fi
mkdir -p "$release_dir/bin" "$release_dir/lib" "$release_dir/systemd" "$release_dir/scripts"
temporary_dir="$(mktemp -d)"
cleanup() { rm -rf "$temporary_dir"; }
trap cleanup EXIT

mvn -f "$release_root/iceberg-loader-java/pom.xml" package
cargo build --manifest-path "$release_root/websocket-extractor-rust/Cargo.toml" --release --locked

curl --fail --location --silent --show-error "$tansu_url" -o "$temporary_dir/tansu.tar.gz"
echo "$tansu_sha256  $temporary_dir/tansu.tar.gz" | sha256sum --check --status
tar -xzf "$temporary_dir/tansu.tar.gz" -C "$release_dir" ./bin/tansu
cp "$release_root/websocket-extractor-rust/target/release/wsr" "$release_dir/bin/wsr"
cp "$release_root/iceberg-loader-java/target/iceberg-loader-0.1.0.jar" "$release_dir/lib/iceberg-loader.jar"
curl --fail --location --silent --show-error "$gcs_connector_url" -o "$release_dir/lib/gcs-connector.jar"
cp "$release_root/deployment/native/ensure-topic.sh" "$release_dir/bin/ensure-topic"
cp "$release_root/deployment/native/install_release.sh" "$release_dir/bin/install-release"
cp "$release_root/deployment/native/run_synthetic.sh" "$release_dir/bin/run-synthetic"
cp "$release_root/deployment/native/systemd/"*.service "$release_dir/systemd/"
cp "$release_root/scripts/run_local_loader.sh" "$release_dir/scripts/run_loader.sh"
chmod 0755 "$release_dir/bin/"* "$release_dir/scripts/run_loader.sh"

(
  cd "$release_dir"
  sha256sum bin/* lib/* scripts/* systemd/* > SHA256SUMS
)
tar -C "$(dirname "$release_dir")" -czf "$release_dir.tar.gz" "$(basename "$release_dir")"
printf 'release_bundle=%s.tar.gz\n' "$release_dir"
