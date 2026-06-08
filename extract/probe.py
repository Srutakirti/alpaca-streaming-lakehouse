"""Observational probe: connects to Alpaca WS, records when bars first arrive.

Logs a single structured `probe_result` event to Cloud Logging on exit so we
can analyze across days/times when Alpaca actually starts emitting bars.
Does not produce to Kafka.
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone

import websockets
from google.cloud import secretmanager

from extractor import get_secret, setup_logging


async def probe(
    uri: str,
    alpaca_key: str,
    alpaca_secret: str,
    symbols: list,
    duration_s: int,
    logger: logging.Logger,
) -> dict:
    probe_started = datetime.now(timezone.utc).isoformat()
    bars_received = 0
    first_bar_at_utc = None
    first_bar_symbol = None
    auth_ok = False
    subscribed_ok = False

    try:
        async with websockets.connect(uri) as ws:
            await ws.recv()  # connection greeting
            await ws.send(json.dumps({"action": "auth", "key": alpaca_key, "secret": alpaca_secret}))
            auth_resp = await ws.recv()
            auth_ok = '"authenticated"' in auth_resp
            logger.info(f"Auth: {auth_resp}")

            await ws.send(json.dumps({"action": "subscribe", "bars": symbols}))
            sub_resp = await ws.recv()
            subscribed_ok = '"subscription"' in sub_resp
            logger.info(f"Subscribed: {sub_resp}")

            deadline = asyncio.get_event_loop().time() + duration_s
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if not isinstance(frame, list):
                    continue

                for item in frame:
                    if isinstance(item, dict) and item.get("T") == "b":
                        bars_received += 1
                        if first_bar_at_utc is None:
                            first_bar_at_utc = datetime.now(timezone.utc).isoformat()
                            first_bar_symbol = item.get("S")
                            logger.info(f"First bar: {item.get('S')} at {first_bar_at_utc}")
    except Exception as e:
        logger.error(f"Probe error: {e}")

    return {
        "event": "probe_result",
        "probe_started_utc": probe_started,
        "first_bar_at_utc": first_bar_at_utc,
        "first_bar_symbol": first_bar_symbol,
        "bars_received": bars_received,
        "duration_s": duration_s,
        "auth_ok": auth_ok,
        "subscribed_ok": subscribed_ok,
        "symbols": symbols,
    }


async def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    log_mode = os.environ.get("LOG_MODE", "both")
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    duration_s = int(os.environ.get("PROBE_DURATION_SECONDS", 120))
    symbols = os.environ.get("ALPACA_SYMBOLS", "AAPL,TSLA").split(",")
    uri = os.environ.get("ALPACA_WS_URI", "wss://stream.data.alpaca.markets/v2/iex")

    logger = setup_logging("alpaca-probe", log_mode, log_level)

    sm_client = secretmanager.SecretManagerServiceClient()
    alpaca_key = get_secret(sm_client, project_id, "ALPACA_KEY")
    alpaca_secret = get_secret(sm_client, project_id, "ALPACA_SECRET")

    result = await probe(uri, alpaca_key, alpaca_secret, symbols, duration_s, logger)
    logger.info(result)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Unhandled: {e}")
        sys.exit(1)
