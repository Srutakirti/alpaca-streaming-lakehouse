from pathlib import Path

import yaml


CONFIG_PATH = Path("deployment/ops-agent/gce-hadoop-catalog-logging.yaml")


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def test_loader_health_has_a_dedicated_ordered_pipeline() -> None:
    config = load_config()
    logging = config["logging"]

    assert logging["receivers"]["gce_hadoop_catalog_loader_json"] == {
        "type": "systemd_journald"
    }
    assert logging["service"]["pipelines"]["gce_hadoop_catalog_loader_json"] == {
        "receivers": ["gce_hadoop_catalog_loader_json"],
        "processors": [
            "exclude_non_loader_health_journal",
            "extract_loader_health_json",
            "parse_loader_health_json",
            "set_loader_health_severity",
        ],
    }


def test_loader_health_pipeline_is_narrow_and_not_duplicated() -> None:
    processors = load_config()["logging"]["processors"]

    assert processors["exclude_non_loader_health_journal"]["match_any"] == [
        'NOT (jsonPayload._SYSTEMD_UNIT = "iceberg-loader.service" AND '
        'jsonPayload.MESSAGE =~ "^LOADER_HEALTH ")',
    ]
    assert processors["extract_loader_health_json"] == {
        "type": "parse_regex",
        "field": "MESSAGE",
        "regex": r"^LOADER_HEALTH (?<loader_health_json>\{.*\})$",
    }
    assert processors["parse_loader_health_json"] == {
        "type": "parse_json",
        "field": "loader_health_json",
    }
    assert processors["set_loader_health_severity"] == {
        "type": "modify_fields",
        "fields": {"severity": {"copy_from": "jsonPayload.level"}},
    }

    plain_processors = load_config()["logging"]["service"]["pipelines"][
        "gce_hadoop_catalog"
    ]["processors"]
    assert "exclude_loader_health_from_plain_journal" in plain_processors
