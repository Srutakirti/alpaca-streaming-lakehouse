"""Docker lifecycle wrapper for one local Tansu broker backed by SQLite."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time

from .config import LocalSettings


class TansuSqlite:
    image = "ghcr.io/tansu-io/tansu:0.6.0"

    def __init__(self, settings: LocalSettings, image: str | None = None) -> None:
        self.settings = settings
        self.image = image or os.environ.get("TANSU_IMAGE", self.image)
        self.process: subprocess.Popen[str] | None = None

    @property
    def container_name(self) -> str:
        return f"gce-hadoop-catalog-tansu-{self.settings.broker_port}"

    @property
    def storage_uri(self) -> str:
        # This is evaluated inside the container; the host directory is mounted
        # at /var/lib/tansu so SQLite persists outside the container.
        return "sqlite:///var/lib/tansu/tansu.sqlite"

    @property
    def command(self) -> list[str]:
        listener = f"tcp://{self.settings.broker_host}:{self.settings.broker_port}"
        return [
            "docker",
            "run",
            "--rm",
            "--name",
            self.container_name,
            "--pull=never",
            "--network",
            "host",
            "--volume",
            f"{self.settings.runtime_dir.resolve() / 'tansu'}:/var/lib/tansu",
            self.image,
            "--kafka-listener-url",
            listener,
            "--kafka-advertised-listener-url",
            listener,
            "--storage-engine",
            self.storage_uri,
        ]

    def start(self, timeout_seconds: float = 15) -> None:
        if self.process is not None:
            return
        if not shutil.which("docker"):
            raise RuntimeError("Docker executable not found")
        self.settings.prepare()
        (self.settings.runtime_dir / "tansu").mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(self.command, text=True)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Tansu exited early with status {self.process.returncode}")
            with socket.socket() as connection:
                connection.settimeout(0.2)
                if connection.connect_ex((self.settings.broker_host, self.settings.broker_port)) == 0:
                    return
            time.sleep(0.1)
        self.stop()
        raise TimeoutError("Tansu did not open its Kafka listener")

    def stop(self) -> None:
        if self.process is None:
            return
        subprocess.run(["docker", "stop", self.container_name], capture_output=True, text=True, check=False)
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        finally:
            self.process = None

    def __enter__(self) -> "TansuSqlite":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()
