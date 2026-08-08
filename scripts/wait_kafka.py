#!/usr/bin/env python3
"""Wait until a Kafka-compatible broker answers metadata requests."""
import argparse
import time

from confluent_kafka.admin import AdminClient


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    deadline = time.time() + args.timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            AdminClient({"bootstrap.servers": args.broker}).list_topics(timeout=2)
            print(f"Kafka ready at {args.broker}")
            return 0
        except Exception as exc:
            last_error = exc
            time.sleep(1)

    print(f"Kafka not ready at {args.broker}: {last_error}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
