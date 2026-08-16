# Multi-stage build for the wsr Rust producer.
# Build context is websocket-extractor-rust/ (standalone Cargo project, not the uv workspace).
#   docker build -t gce-hadoop-catalog-websocket-extractor:local websocket-extractor-rust
#
# Build stage needs cmake + ssl/sasl headers for the vendored librdkafka
# (rdkafka "cmake-build" feature). Runtime stage needs the matching shared
# libs plus ca-certificates for the wss:// TLS handshake to Alpaca.
FROM rust:bookworm AS build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo test --locked && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/wsr /usr/local/bin/wsr
ENTRYPOINT ["/usr/local/bin/wsr"]
