# Commit Instructions

Dont add co authored by claude code in commit messages.

Keep the commit messages concise , sharp and clean. Dont overly bloat the messages. 15-20 words is the max.

# Commands

- `make test` runs the fast Python suite and Rust unit tests.
- `make test-integration` starts local Tansu and runs Kafka/Iceberg integration tests.
- `make e2e` runs synthetic generator -> Tansu -> loader -> Iceberg -> frontend API.
- `make lint` runs Python ruff plus Rust fmt/clippy.
