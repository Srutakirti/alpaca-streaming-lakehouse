import asyncio
import websockets
import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from confluent_kafka import Producer
from google.cloud import secretmanager
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers.transports.background_thread import BackgroundThreadTransport
import os


@dataclass
class Metrics:
    messages_received: int = 0
    messages_sent: int = 0
    delivery_failures: int = 0
    errors: int = 0
    last_message_ts: Optional[str] = None
    last_delivery_ts: Optional[str] = None
    connection_status: bool = False

    def snapshot(self) -> Dict[str, Any]:
        return {
            "component": "alpaca-extractor",
            "messages_received": self.messages_received,
            "messages_sent": self.messages_sent,
            "delivery_failures": self.delivery_failures,
            "errors": self.errors,
            "last_message_ts": self.last_message_ts,
            "last_delivery_ts": self.last_delivery_ts,
            "connection_status": self.connection_status,
        }


def get_secret(client: secretmanager.SecretManagerServiceClient, project_id: str, secret_id: str) -> str:
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("utf-8")


def setup_logging(name: str, mode: str = "both", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    if mode in ("cloud", "both"):
        gcp_client = google.cloud.logging.Client()
        logger.addHandler(CloudLoggingHandler(gcp_client, name=name, transport=BackgroundThreadTransport))

    if mode in ("stdout", "both"):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(handler)

    return logger


def make_delivery_callback(metrics: Metrics) -> Any:
    def on_delivery(err, msg):
        if err:
            metrics.delivery_failures += 1
        else:
            metrics.messages_sent += 1
            metrics.last_delivery_ts = datetime.utcnow().isoformat() + "Z"
    return on_delivery


def metrics_emitter(metrics: Metrics, logger: logging.Logger, interval: int, stop: threading.Event) -> None:
    while not stop.wait(interval):
        logger.info(metrics.snapshot())


async def stream(
    producer: Producer,
    topic: str,
    alpaca_key: str,
    alpaca_secret: str,
    symbols: list,
    config: dict,
    shutdown_event: asyncio.Event,
    logger: logging.Logger,
    metrics: Metrics,
) -> None:
    loop = asyncio.get_event_loop()
    delivery_cb = make_delivery_callback(metrics)
    backoff = 1
    retry_count = 0
    max_retries = config.get("max_retries", 5)
    timeout = config.get("timeout", 120)
    backoff_max = config.get("backoff_max", 120)
    uri = config["websocket_uri"]

    while not shutdown_event.is_set() and retry_count < max_retries:
        try:
            async with websockets.connect(uri) as ws:
                logger.info("Connected to Alpaca WebSocket")
                metrics.connection_status = True

                await ws.recv()
                await ws.send(json.dumps({"action": "auth", "key": alpaca_key, "secret": alpaca_secret}))
                logger.info(f"Auth: {await ws.recv()}")

                await ws.send(json.dumps({"action": "subscribe", "bars": symbols}))
                logger.info(f"Subscribed: {await ws.recv()}")

                backoff = 1

                while not shutdown_event.is_set():
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        metrics.messages_received += 1
                        metrics.last_message_ts = datetime.utcnow().isoformat() + "Z"

                        producer.produce(topic, value=message.encode("utf-8"), callback=delivery_cb)
                        await loop.run_in_executor(None, lambda: producer.poll(0))

                    except asyncio.TimeoutError:
                        logger.warning(f"No message for {timeout}s, retry {retry_count}")
                        retry_count += 1
                        break

        except Exception as e:
            metrics.connection_status = False
            metrics.errors += 1
            retry_count += 1
            logger.error(f"Error: {e}. Retry {retry_count}/{max_retries}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)

    metrics.connection_status = False
    logger.info("Exiting stream loop")


async def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "alpaca-bars")
    log_name = os.environ.get("LOG_NAME", "alpaca-stream")
    log_mode = os.environ.get("LOG_MODE", "both")
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    metrics_interval = int(os.environ.get("METRICS_INTERVAL", 10))
    symbols = os.environ.get("ALPACA_SYMBOLS", "AAPL,TSLA").split(",")

    alpaca_config = {
        "websocket_uri": os.environ.get("ALPACA_WS_URI", "wss://stream.data.alpaca.markets/v2/iex"),
        "max_retries": int(os.environ.get("MAX_RETRIES", 5)),
        "timeout": int(os.environ.get("TIMEOUT", 120)),
        "backoff_max": int(os.environ.get("BACKOFF_MAX", 120)),
    }

    logger = setup_logging(log_name, log_mode, log_level)

    sm_client = secretmanager.SecretManagerServiceClient()
    alpaca_key = get_secret(sm_client, project_id, "ALPACA_KEY")
    alpaca_secret = get_secret(sm_client, project_id, "ALPACA_SECRET")

    producer = Producer({"bootstrap.servers": kafka_broker})

    shutdown_event = asyncio.Event()
    metrics = Metrics()

    stop_emitter = threading.Event()
    emitter_thread = threading.Thread(
        target=metrics_emitter,
        args=(metrics, logger, metrics_interval, stop_emitter),
        daemon=True,
    )
    emitter_thread.start()

    def handle_shutdown(*_):
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handle_shutdown)

    try:
        await stream(
            producer=producer,
            topic=kafka_topic,
            alpaca_key=alpaca_key,
            alpaca_secret=alpaca_secret,
            symbols=symbols,
            config=alpaca_config,
            shutdown_event=shutdown_event,
            logger=logger,
            metrics=metrics,
        )
    except Exception as e:
        logger.error(f"Fatal: {e}")
    finally:
        stop_emitter.set()
        producer.flush(timeout=10)
        logger.info(f"Final metrics: {metrics.snapshot()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Unhandled: {e}")
        sys.exit(1)
