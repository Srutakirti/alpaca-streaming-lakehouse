# Native VM release

This is the Docker-free deployment format for the e2-micro VM. The VM runs:

- the pinned Tansu Linux x86_64 executable;
- the Rust WebSocket extractor (`wsr`) built by `cargo --release --locked`;
- the shaded Java loader JAR and its separate shaded GCS Hadoop connector JAR;
- systemd units and `flock` for a single writer.

Nothing on the VM builds the project. Docker, Maven, Cargo, UV, and Spark are not installed there.

## Build a bundle

Build on a Linux x86_64 host (or a compatible build runner), after obtaining and verifying the official Tansu 0.6.0 Linux x86_64 executable:

```bash
TANSU_BINARY=/absolute/path/to/tansu-0.6.0-linux-x86_64 \
  deployment/native/build_release.sh
```

The output is `dist/gce-hadoop-catalog-<git-sha>.tar.gz`. It contains only the VM runtime files and a `SHA256SUMS` manifest. `build_release.sh` never overwrites an existing bundle.

The Rust binary dynamically links OpenSSL and zlib. The Terraform bootstrap requests `libssl3` and `zlib1g`; Ubuntu 24.04 resolves the OpenSSL runtime as `libssl3t64`, which provides `libssl.so.3`.

## Install on the VM

The Terraform bootstrap creates the `gcehcatalog` system user, `/opt/gce-hadoop-catalog`, and `/etc/gce-hadoop-catalog/runtime.env`. Copy the archive to the VM, extract only long enough to access its installer, then run:

```bash
release=/tmp/gce-hadoop-catalog-<git-sha>.tar.gz
temporary_dir=$(mktemp -d)
tar -xzf "$release" -C "$temporary_dir"
sudo "$temporary_dir"/gce-hadoop-catalog-<git-sha>/bin/install-release "$release"
rm -rf "$temporary_dir"
```

The installation verifies hashes, atomically updates the `current` symlink, and reloads systemd. It does not enable or start the pipeline. Starting services remains an explicit acceptance-test step.

`runtime.env.example` documents every non-secret runtime setting. The VM uses its attached service account and Application Default Credentials to access the GCS warehouse; no service-account key is copied into the bundle.
