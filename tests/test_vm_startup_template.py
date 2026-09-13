from pathlib import Path


STARTUP_TEMPLATE = Path("terraform/templates/startup.sh.tftpl")
ALPACA_ENV_EXAMPLE = Path("deployment/native/alpaca.env.example")


def environment_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        values[name] = value.strip('"')
    return values


def test_cloud_runtime_matches_direct_extractor_topic_and_table() -> None:
    runtime = environment_values(STARTUP_TEMPLATE)
    extractor = environment_values(ALPACA_ENV_EXAMPLE)

    assert runtime["KAFKA_TOPIC"] == extractor["KAFKA_TOPIC"]
    assert runtime["KAFKA_TOPIC"] == "alpaca-bars-direct-candidate"
    assert runtime["KAFKA_GROUP_ID"] == "alpaca-iceberg-loader-direct-candidate"
    assert runtime["ICEBERG_NAMESPACE"] == "alpaca_candidate"
    assert runtime["ICEBERG_TABLE"] == "bars_direct"


def test_cloud_runtime_bounds_loader_heap() -> None:
    options = environment_values(STARTUP_TEMPLATE)["LOADER_JAVA_OPTS"]

    assert "-Xms128m" in options
    assert "-Xmx384m" in options
    assert "ClientTelemetryReporter=warn" in options
