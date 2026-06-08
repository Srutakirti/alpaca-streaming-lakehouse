#!/usr/bin/env python3
"""Test Tansu Kafka broker by producing and consuming messages."""

import argparse
import sys
import time
import uuid
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def wait_for_broker(broker: str, max_retries: int = 30, retry_delay: int = 2) -> bool:
    """Wait for the Kafka broker to become available."""
    print(f"Waiting for broker at {broker}...")
    for attempt in range(max_retries):
        try:
            admin = AdminClient({'bootstrap.servers': broker})
            metadata = admin.list_topics(timeout=5)
            print(f"Broker is available after {attempt + 1} attempts")
            return True
        except KafkaException as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries}: Broker not ready ({e}), retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries}: Error ({e}), retrying in {retry_delay}s...")
                time.sleep(retry_delay)
    return False


def create_topic(broker: str, topic: str) -> bool:
    """Create a Kafka topic."""
    print(f"Creating topic '{topic}'...")
    try:
        admin = AdminClient({'bootstrap.servers': broker})
        new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
        futures = admin.create_topics([new_topic])
        for t, future in futures.items():
            try:
                future.result()
                print(f"  Topic '{t}' created successfully")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"  Topic '{t}' already exists")
                else:
                    raise
        return True
    except Exception as e:
        print(f"  Failed to create topic: {e}")
        return False


def produce_messages(broker: str, topic: str, messages: list[str]) -> bool:
    """Produce messages to the Kafka topic."""
    print(f"Producing {len(messages)} messages to '{topic}'...")
    delivered = []

    def delivery_callback(err, msg):
        if err:
            print(f"  Delivery failed: {err}")
        else:
            delivered.append(msg.value().decode('utf-8'))
            print(f"  Sent: {msg.value().decode('utf-8')} -> partition={msg.partition()}, offset={msg.offset()}")

    try:
        producer = Producer({
            'bootstrap.servers': broker,
            'socket.timeout.ms': 10000,
        })
        for msg in messages:
            producer.produce(topic, value=msg.encode('utf-8'), callback=delivery_callback)
        producer.flush(timeout=30)
        print(f"  All {len(delivered)} messages produced successfully")
        return len(delivered) == len(messages)
    except Exception as e:
        print(f"  Failed to produce messages: {e}")
        return False


def consume_messages(broker: str, topic: str, expected_count: int, timeout: int = 30) -> list[str]:
    """Consume messages from the Kafka topic."""
    print(f"Consuming messages from '{topic}' (timeout: {timeout}s)...")
    messages = []
    try:
        consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': f'tansu-test-{uuid.uuid4().hex[:8]}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        consumer.subscribe([topic])
        start_time = time.time()
        while time.time() - start_time < timeout and len(messages) < expected_count:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"  Consumer error: {msg.error()}")
                break
            value = msg.value().decode('utf-8')
            messages.append(value)
            print(f"  Received: {value} from partition={msg.partition()}, offset={msg.offset()}")
        consumer.close()
        print(f"  Consumed {len(messages)} messages")
    except Exception as e:
        print(f"  Error consuming messages: {e}")
    return messages


def run_test(broker: str) -> bool:
    """Run the complete Kafka test."""
    print("=" * 60)
    print("Tansu Kafka Test Suite")
    print("=" * 60)
    print(f"Broker: {broker}")
    print()

    # Wait for broker
    if not wait_for_broker(broker):
        print("FAILED: Broker not available")
        return False
    print()

    # Create unique topic for this test run
    topic = f"tansu-test-{uuid.uuid4().hex[:8]}"
    if not create_topic(broker, topic):
        print("FAILED: Could not create topic")
        return False
    print()

    # Give Tansu a moment to register the topic
    time.sleep(2)

    # Produce messages
    test_messages = [
        "Hello from Tansu!",
        "Message 2: Testing producer",
        "Message 3: End-to-end test"
    ]
    if not produce_messages(broker, topic, test_messages):
        print("FAILED: Could not produce messages")
        return False
    print()

    # Consume messages
    received = consume_messages(broker, topic, len(test_messages))
    print()

    # Verify
    print("Verification:")
    if len(received) == len(test_messages):
        if received == test_messages:
            print("  All messages received in order")
            print()
            print("=" * 60)
            print("TEST PASSED: Tansu Kafka broker is working correctly!")
            print("=" * 60)
            return True
        else:
            print("  Messages received but order differs")
            print(f"  Expected: {test_messages}")
            print(f"  Received: {received}")
    else:
        print(f"  Message count mismatch: expected {len(test_messages)}, got {len(received)}")

    print()
    print("=" * 60)
    print("TEST FAILED")
    print("=" * 60)
    return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test Tansu Kafka broker")
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)"
    )
    args = parser.parse_args()

    success = run_test(args.broker)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
