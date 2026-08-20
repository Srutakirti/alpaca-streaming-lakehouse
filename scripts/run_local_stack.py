#!/usr/bin/env python3
"""Run a selected Alpaca-compatible producer through Tansu and the catalog-neutral loader."""

from __future__ import annotations

import os
import select
import subprocess
import sys
import tempfile
import time
from argparse import ArgumentParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.runtime import ensure_topic, publish
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars
from gce_hadoop_catalog.tansu_sqlite import TansuSqlite


ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / "iceberg-loader-java/target/iceberg-loader-0.1.0.jar"
WSR_IMAGE = "gce-hadoop-catalog-websocket-extractor:local"


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


def parser() -> ArgumentParser:
    result = ArgumentParser()
    result.add_argument("--source", choices=("synthetic", "fakepaca-wsr"), default="synthetic")
    result.add_argument(
        "--runtime-dir",
        type=Path,
        help="persistent runtime directory; defaults to a fresh temporary directory for each acceptance run",
    )
    return result


def start_wsr(environment: dict[str, str], port: int) -> subprocess.Popen[str]:
    missing = [name for name in ("ALPACA_KEY", "ALPACA_SECRET") if not environment.get(name)]
    if missing:
        raise RuntimeError("fakepaca-wsr requires " + ", ".join(missing))
    exists = subprocess.run(["docker", "image", "inspect", WSR_IMAGE], capture_output=True, check=False)
    if exists.returncode:
        raise RuntimeError("build WSR first: docker build -t " + WSR_IMAGE + " websocket-extractor-rust")
    command = [
        "docker", "run", "--rm", "--network", "host", "--name", f"gce-hcatalog-wsr-{port}",
        "-e", "KAFKA_BROKER", "-e", "KAFKA_TOPIC", "-e", "ALPACA_KEY", "-e", "ALPACA_SECRET",
        "-e", "ALPACA_WS_URI=wss://stream.data.alpaca.markets/v2/test",
        "-e", "ALPACA_SYMBOLS=FAKEPACA", "-e", "DATA_IDLE_TIMEOUT=0", WSR_IMAGE,
    ]
    return subprocess.Popen(command, cwd=ROOT, env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)


def main() -> None:
    arguments = parser().parse_args()
    source = arguments.source
    if not JAR.is_file():
        raise SystemExit("build the loader first: mvn -f iceberg-loader-java/pom.xml package")
    explicit_runtime = arguments.runtime_dir or (
        Path(os.environ["PIPELINE_RUNTIME_DIR"]) if "PIPELINE_RUNTIME_DIR" in os.environ else None
    )
    temporary_runtime = None if explicit_runtime else tempfile.TemporaryDirectory(prefix="gce-hcatalog-acceptance-")
    runtime_dir = explicit_runtime or Path(temporary_runtime.name)
    settings = LocalSettings(runtime_dir=runtime_dir.resolve())
    settings.prepare()
    warehouse = settings.warehouse_dir.resolve().as_uri()
    environment = os.environ | {
        "PIPELINE_RUNTIME_DIR": str(settings.runtime_dir),
        "KAFKA_BROKER": settings.broker_url,
        "KAFKA_TOPIC": settings.topic,
        "KAFKA_GROUP_ID": f"local-acceptance-{settings.runtime_dir.name}",
        "ICEBERG_WAREHOUSE": warehouse,
        "LOADER_MAX_RECORDS": "12" if source == "synthetic" else "1",
        "LOADER_MAX_SECONDS": "300",
    }
    try:
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
        producer: subprocess.Popen[str] | None = None
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
            expected = 12
            if source == "synthetic":
                publish(settings, generate_bars(start=default_start(), periods=6))
            else:
                expected = 1
                producer = start_wsr(environment, settings.broker_port)
            deadline = time.monotonic() + (45 if source == "synthetic" else 150)
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
                if f"received={expected} inserted={expected}" in line:
                    committed = True
                    break
            if not committed:
                raise TimeoutError("loader did not commit the synthetic batch")
        finally:
            if producer is not None:
                producer.terminate()
                try:
                    producer.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()
            loader.terminate()
            try:
                loader.wait(timeout=15)
            except subprocess.TimeoutExpired:
                loader.kill()
                loader.wait()
        inspected = subprocess.run(
            ["java", "-cp", str(JAR), "io.gcehcatalog.loader.TableInspector"],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        print(inspected.stdout, end="")
        record_count = next(
            (line for line in inspected.stdout.splitlines() if line.startswith("record_count=")),
            None,
        )
        if record_count != f"record_count={expected}":
            raise RuntimeError(f"unexpected table contents: {inspected.stdout.strip()}")
      print(f"local_acceptance=passed source={source} flock=passed")
    finally:
        if temporary_runtime is not None:
            temporary_runtime.cleanup()


if __name__ == "__main__":
    main()
