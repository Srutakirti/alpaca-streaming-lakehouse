#!/usr/bin/env bash
# Install an already-verified native release archive on an Ubuntu VM.
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi
if [[ "$#" -ne 1 || ! -f "$1" ]]; then
  echo "usage: $0 /path/to/gce-hadoop-catalog-<release>.tar.gz" >&2
  exit 1
fi

archive="$1"
install_root="/opt/gce-hadoop-catalog/releases"
staging_dir="$(mktemp -d)"
cleanup() { rm -rf "$staging_dir"; }
trap cleanup EXIT

tar -xzf "$archive" -C "$staging_dir"
release_dir="$(find "$staging_dir" -mindepth 1 -maxdepth 1 -type d -print -quit)"
if [[ -z "$release_dir" || ! -f "$release_dir/SHA256SUMS" ]]; then
  echo "release archive does not contain a signed-layout bundle" >&2
  exit 1
fi
(cd "$release_dir" && sha256sum --check SHA256SUMS)

install -d -m 0755 "$install_root"
target="$install_root/$(basename "$release_dir")"
if [[ -e "$target" ]]; then
  echo "release already installed: $target" >&2
  exit 1
fi
mv "$release_dir" "$target"
ln -sfn "$target" /opt/gce-hadoop-catalog/current
install -m 0644 "$target"/systemd/*.service "$target"/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
printf 'installed_release=%s\n' "$target"
