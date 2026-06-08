"""Debug consumer: tail messages from a Kafka topic and print to stdout.

Examples:
    python scripts/peek_kafka.py
    python scripts/peek_kafka.py --broker localhost:9092 --topic alpaca-bars
    python scripts/peek_kafka.py --from-beginning --max 50
    python scripts/peek_kafka.py --raw            # don't pretty-print JSON
"""

import argparse
import json
import signal
import sys
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(broker: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        return
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
            print(f"[peek] created topic {t}", file=sys.stderr)
        except Exception as e:
            print(f"[peek] create_topics({t}) note: {e}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--topic", default="alpaca-bars")
    p.add_argument("--group", default=f"debug-peek-{uuid.uuid4().hex[:8]}")
    p.add_argument("--from-beginning", action="store_true",
                   help="Read from the earliest offset (default: tail new messages)")
    p.add_argument("--max", type=int, default=0, help="Stop after N messages (0 = unlimited)")
    p.add_argument("--raw", action="store_true", help="Print raw bytes instead of pretty JSON")
    p.add_argument("--timeout", type=float, default=1.0, help="poll() timeout in seconds")
    args = p.parse_args()

    ensure_topic(args.broker, args.topic)

    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,
        # Tansu quirk: long-poll Fetch can stall past rdkafka's default 60s
        # socket.timeout.ms and trigger REQTMOUT. Keep fetch waits short.
        "fetch.wait.max.ms": 500,
        "socket.timeout.ms": 30000,
    })

    # Show low/high watermarks so it's obvious whether the topic actually has data
    # and where this consumer will start reading from.
    md = consumer.list_topics(args.topic, timeout=10)
    for pid in md.topics[args.topic].partitions:
        lo, hi = consumer.get_watermark_offsets(
            TopicPartition(args.topic, pid), timeout=10, cached=False)
        print(f"[peek] partition={pid} low={lo} high={hi} "
              f"(messages_in_partition={hi - lo})", file=sys.stderr)

    if args.from_beginning:
        # Assign all partitions at offset 0 so we don't depend on group state.
        md = consumer.list_topics(args.topic, timeout=10)
        parts = [TopicPartition(args.topic, pid, 0) for pid in md.topics[args.topic].partitions]
        consumer.assign(parts)
    else:
        consumer.subscribe([args.topic])

    print(f"[peek] broker={args.broker} topic={args.topic} group={args.group} "
          f"from_beginning={args.from_beginning}", file=sys.stderr)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    seen = 0
    try:
        while not stop["flag"]:
            msg = consumer.poll(args.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[peek] error: {msg.error()}", file=sys.stderr)
                continue

            value = msg.value()
            header = (f"--- offset={msg.offset()} partition={msg.partition()} "
                      f"ts={msg.timestamp()[1]} key={msg.key()!r}")
            print(header)
            if args.raw or value is None:
                print(value)
            else:
                try:
                    print(json.dumps(json.loads(value), indent=2))
                except (ValueError, TypeError):
                    print(value.decode("utf-8", errors="replace"))

            seen += 1
            if args.max and seen >= args.max:
                break
    finally:
        consumer.close()
        print(f"[peek] consumed {seen} message(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
