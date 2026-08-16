#!/usr/bin/env python3
"""Run the complete local acceptance flow against Tansu SQLite and HadoopCatalog."""

from __future__ import annotations

import os
import select
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.runtime import ensure_topic, publish
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars
from gce_hadoop_catalog.tansu_sqlite import TansuSqlite


ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / "loader-java/target/iceberg-loader-0.1.0.jar"


def wait_for_writer_lock(lock_path: Path, loader: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if loader.poll() is not None:
            raise RuntimeError("loader exited before acquiring its writer lock")
        probe = subprocess.run(
            ["flock", "-n", str(lock_path), "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode != 0:
            return
        time.sleep(0.1)
    raise TimeoutError("loader did not acquire its writer lock")


def main() -> None:
    if not JAR.is_file():
        raise SystemExit("build the loader first: mvn -f loader-java/pom.xml package")
    settings = LocalSettings.from_environment()
    settings.prepare()
    warehouse = settings.warehouse_dir.resolve().as_uri()
    environment = os.environ | {
        "PIPELINE_RUNTIME_DIR": str(settings.runtime_dir),
        "KAFKA_BROKER": settings.broker_url,
        "KAFKA_TOPIC": settings.topic,
        "KAFKA_GROUP_ID": f"local-acceptance-{settings.broker_port}",
        "ICEBERG_WAREHOUSE": warehouse,
        "LOADER_MAX_RECORDS": "12",
        "LOADER_MAX_SECONDS": "300",
    }
    with TansuSqlite(settings):
        ensure_topic(settings)
        loader = subprocess.Popen(
            [str(ROOT / "scripts/run_local_loader.sh")],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        try:
            wait_for_writer_lock(settings.runtime_dir / "loader.lock", loader)
            # This must fail while the first process owns the writer lock.
            duplicate = subprocess.run(
                [str(ROOT / "scripts/run_local_loader.sh")],
                cwd=ROOT,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            if duplicate.returncode == 0:
                raise RuntimeError("flock allowed a second loader")
            publish(settings, generate_bars(start=default_start(), periods=6))
            deadline = time.monotonic() + 45
            assert loader.stdout is not None
            committed = False
            while time.monotonic() < deadline:
                ready, _, _ = select.select([loader.stdout], [], [], 1)
                if not ready:
                    if loader.poll() is not None:
                        raise RuntimeError("loader exited before committing the synthetic batch")
                    continue
                line = loader.stdout.readline()
                if not line:
                    raise RuntimeError("loader exited before committing the synthetic batch")
                print(line, end="")
                if "received=12 inserted=12" in line:
                    committed = True
                    break
            if not committed:
                raise TimeoutError("loader did not commit the synthetic batch")
        finally:
            loader.terminate()
            try:
                loader.wait(timeout=15)
            except subprocess.TimeoutExpired:
                loader.kill()
                loader.wait()
        inspected = subprocess.run(
            ["java", "-cp", str(JAR), "io.gcehcatalog.loader.TableInspector", warehouse],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        print(inspected.stdout, end="")
        if inspected.stdout.strip() != "record_count=12":
            raise RuntimeError(f"unexpected table contents: {inspected.stdout.strip()}")
    print("local_acceptance=passed flock=passed")


if __name__ == "__main__":
    main()
