# Repository Guidelines

## Project Structure & Module Organization

Python pipeline helpers live under `src/gce_hadoop_catalog/`, with entry-point scripts in `scripts/` and pytest coverage in `tests/`. The Java Iceberg consumer is isolated in `iceberg-loader-java/`; the Rust Alpaca WebSocket producer is in `websocket-extractor-rust/`. The TypeScript/Vite dashboard lives in `dashboard/`. Shared wire contracts are under `contracts/`, infrastructure is in `terraform/` and `deployment/`, and operational guidance belongs in `docs/` or `notebooks/`.

Keep changes within the owning component unless a contract or end-to-end behavior genuinely spans components. Do not commit generated output such as `target/`, `dist/`, local warehouses, credentials, or runtime databases.

## Build, Test, and Development Commands

- `uv sync --dev`: install the Python 3.12 environment from `uv.lock`.
- `uv run pytest`: run Python unit tests; integration-marked tests are excluded by default.
- `mvn -f iceberg-loader-java/pom.xml test`: run Java/JUnit 5 tests.
- `mvn -f iceberg-loader-java/pom.xml package`: build the shaded Java 17 loader JAR.
- `cargo test --manifest-path websocket-extractor-rust/Cargo.toml`: test the Rust extractor.
- `cargo fmt --manifest-path websocket-extractor-rust/Cargo.toml -- --check`: verify Rust formatting.
- `npm --prefix dashboard ci && npm --prefix dashboard run build`: install locked dashboard dependencies, type-check, and build.
- `uv run python scripts/run_local_stack.py --source synthetic`: exercise the bounded local Tansu-to-Iceberg flow; Docker is required.

## Coding Style & Naming Conventions

Use four spaces and `snake_case` for Python; Ruff is configured for 100-character lines. Follow standard Java conventions (`UpperCamelCase` classes, `lowerCamelCase` members) and preserve the `io.gcehcatalog.loader` package. Rust code must pass `cargo fmt`; use `snake_case` functions/modules and `UpperCamelCase` types. TypeScript uses two-space indentation and camelCase identifiers. Keep configuration environment-driven, and never log secrets.

## Testing Guidelines

Name Python tests `test_*.py` and Java tests `*Test.java`. Add focused unit tests beside the affected component and update fixtures in `tests/fixtures/` when dashboard input contracts change. Mark Docker/Tansu-dependent pytest cases with `@pytest.mark.integration`; run them explicitly with `uv run pytest -m integration`.

## Agent-Specific Script Logging

Whenever an agent runs a repository script, capture standard output and errors in a timestamped file under `.logs/`. For example: `mkdir -p .logs && scripts/run_local_tansu.sh 2>&1 | tee .logs/run_local_tansu-YYYYMMDD-HHMMSS.log`. Always report the exact path and show or summarize the log so the user can check progress. Redact credentials and secrets before sharing logs.

## Commit & Pull Request Guidelines

History follows Conventional Commit prefixes such as `feat:`, `fix:`, `docs:`, and `chore:`. Write imperative, narrowly scoped subjects. Pull requests should explain the behavior change, list verification commands, link relevant issues or plans, and call out configuration or contract changes. Include screenshots for dashboard UI changes and never include `.env` files or Alpaca credentials.
