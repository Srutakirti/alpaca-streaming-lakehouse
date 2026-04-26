#!/usr/bin/env python3
"""
Synthetic Stock Event Generator

Generates realistic stock bar events and sends them to Kafka for testing purposes.
Mimics the format of Alpaca WebSocket bar messages.

Usage:
    uv run --package extract python extract/helpers/synthetic_stock_generator.py --symbols AAPL TSLA GOOGL --rate 10
    uv run --package extract python extract/helpers/synthetic_stock_generator.py --help
"""

import json
import random
import time
import argparse
import sys
import signal
import threading
from datetime import datetime
from typing import List, Dict, Any

from confluent_kafka import Producer


STOCK_UNIVERSE = {
    'AAPL': {'base': 175.0, 'volatility': 5.0},
    'TSLA': {'base': 242.0, 'volatility': 15.0},
    'GOOGL': {'base': 141.0, 'volatility': 8.0},
    'MSFT': {'base': 378.0, 'volatility': 10.0},
    'AMZN': {'base': 157.0, 'volatility': 8.0},
    'NVDA': {'base': 495.0, 'volatility': 25.0},
    'META': {'base': 352.0, 'volatility': 12.0},
    'NFLX': {'base': 488.0, 'volatility': 20.0},
    'AMD': {'base': 143.0, 'volatility': 10.0},
    'INTC': {'base': 48.0, 'volatility': 3.0},
    'SPY': {'base': 475.0, 'volatility': 5.0},
    'QQQ': {'base': 395.0, 'volatility': 8.0},
    'DIA': {'base': 375.0, 'volatility': 4.0},
    'IWM': {'base': 195.0, 'volatility': 5.0},
}


class StockPriceSimulator:
    def __init__(self, symbol: str, base_price: float, volatility: float):
        self.symbol = symbol
        self.current_price = base_price
        self.volatility = volatility

    def get_next_bar(self) -> Dict[str, Any]:
        change_pct = random.gauss(0, self.volatility / 100)
        base_price = STOCK_UNIVERSE.get(self.symbol, {}).get('base', self.current_price)
        reversion_force = (base_price - self.current_price) * 0.01
        self.current_price += self.current_price * change_pct + reversion_force

        open_price = self.current_price * (1 + random.gauss(0, 0.001))
        spread = abs(random.gauss(0, self.volatility / 200))
        high_price = max(self.current_price * (1 + spread), self.current_price, open_price)
        low_price = min(self.current_price * (1 - spread), self.current_price, open_price)
        volume = int(random.lognormvariate(7, 1.5)) * 100
        vwap = (high_price + low_price + self.current_price) / 3

        return {
            'T': 'b',
            'S': self.symbol,
            'o': round(open_price, 2),
            'h': round(high_price, 2),
            'l': round(low_price, 2),
            'c': round(self.current_price, 2),
            'v': volume,
            't': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'n': max(1, int(volume / random.randint(50, 200))),
            'vw': round(vwap, 2),
        }


class _Metrics:
    def __init__(self):
        self.messages_sent = 0
        self.errors = 0
        self.last_send_ts: str = ""


def _delivery_callback(metrics: _Metrics, err, msg):
    if err:
        metrics.errors += 1
        print(f"Delivery error: {err}", file=sys.stderr)
    else:
        metrics.messages_sent += 1
        metrics.last_send_ts = datetime.utcnow().isoformat() + "Z"


def _metrics_emitter(metrics: _Metrics, interval: int, stop: threading.Event, symbols: List[str]) -> None:
    while not stop.wait(interval):
        print(
            f"[{datetime.utcnow().strftime('%H:%M:%S')}] "
            f"sent={metrics.messages_sent} errors={metrics.errors} "
            f"symbols={','.join(symbols)} last_ts={metrics.last_send_ts}"
        )


def run(symbols: List[str], kafka_bootstrap: str, topic: str, rate: float, metrics_interval: int) -> None:
    simulators = {}
    for symbol in symbols:
        info = STOCK_UNIVERSE.get(symbol, {'base': 100.0, 'volatility': 5.0})
        simulators[symbol] = StockPriceSimulator(symbol, info['base'], info['volatility'])

    metrics = _Metrics()
    producer = Producer({"bootstrap.servers": kafka_bootstrap})

    stop = threading.Event()
    emitter = threading.Thread(
        target=_metrics_emitter,
        args=(metrics, metrics_interval, stop, symbols),
        daemon=True,
    )
    emitter.start()

    def handle_shutdown(*_):
        stop.set()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    print(f"Connected to {kafka_bootstrap}, publishing to topic '{topic}'")
    print(f"Symbols: {', '.join(symbols)} | Rate: {rate} iter/sec | Press Ctrl+C to stop\n")

    interval = 1.0 / rate
    try:
        while not stop.is_set():
            t0 = time.monotonic()
            bars = [simulators[s].get_next_bar() for s in symbols]
            payload = json.dumps(bars).encode("utf-8")
            producer.produce(topic, value=payload, callback=lambda err, msg: _delivery_callback(metrics, err, msg))
            producer.poll(0)
            elapsed = time.monotonic() - t0
            time.sleep(max(0.0, interval - elapsed))
    finally:
        stop.set()
        producer.flush(timeout=10)
        print(f"\nDone. Total sent: {metrics.messages_sent} | Errors: {metrics.errors}")


def main():
    parser = argparse.ArgumentParser(description="Synthetic Alpaca bar generator → Kafka")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "TSLA", "GOOGL", "MSFT", "NVDA"])
    parser.add_argument("--rate", type=float, default=5.0, help="Iterations per second (default 5.0)")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="alpaca-bars", help="Kafka topic")
    parser.add_argument("--metrics-interval", type=int, default=10, help="Seconds between metrics printout")
    args = parser.parse_args()

    unknown = [s for s in args.symbols if s not in STOCK_UNIVERSE]
    if unknown:
        print(f"Note: {unknown} not in STOCK_UNIVERSE, using defaults (base=100, vol=5%)")

    run(args.symbols, args.kafka, args.topic, args.rate, args.metrics_interval)


if __name__ == "__main__":
    main()
