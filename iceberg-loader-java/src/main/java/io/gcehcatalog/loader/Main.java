package io.gcehcatalog.loader;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** One long-lived Kafka consumer; it commits offsets only after an Iceberg commit. */
public final class Main {
  public static void main(String[] args) throws Exception {
    // Load the transport, catalog, and bounded-batch settings used for this loader process.
    String broker = env("KAFKA_BROKER", "127.0.0.1:19092");
    String topic = env("KAFKA_TOPIC", "alpaca-bars");
    CatalogConfig catalogConfig = CatalogConfig.fromEnvironment();
    int maxRecords = Integer.parseInt(env("LOADER_MAX_RECORDS", "100"));
    int maxPollRecords = Integer.parseInt(env("LOADER_MAX_POLL_RECORDS", "1"));
    if (maxPollRecords != 1) throw new IllegalArgumentException("LOADER_MAX_POLL_RECORDS must be 1 to preserve bounded source-record commits");
    Duration maxWait = Duration.ofSeconds(Long.parseLong(env("LOADER_MAX_SECONDS", "300")));

    // Configure an at-least-once Kafka consumer whose offsets are committed explicitly.
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, env("KAFKA_GROUP_ID", "alpaca-iceberg-loader"));
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    // Coordinate graceful termination so pending data can be flushed before resources close.
    AtomicBoolean running = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "loader-shutdown"));

    // Keep one Kafka consumer and one Iceberg writer open for the lifetime of the process.
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
         IcebergWriter writer = new IcebergWriter(catalogConfig)) {
      consumer.subscribe(List.of(topic));
      BoundedBatch batch = new BoundedBatch(maxRecords);
      long openedAt = System.nanoTime();

      // Decode Kafka frames into bars and flush each batch as soon as it reaches its size limit.
      while (running.get()) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
        for (var record : records) {
          BoundedBatch.Source source = new BoundedBatch.Source(record.topic(), record.partition(), record.offset());
          for (BoundedBatch.Batch fullBatch : batch.add(source, AlpacaBar.decodeFrame(record.value()))) {
            flush(consumer, writer, fullBatch);
            openedAt = System.nanoTime();
          }
        }

        // Flush a partial batch after the time limit so low-volume data is not held indefinitely.
        if (!batch.isEmpty() && elapsed(openedAt).compareTo(maxWait) >= 0) {
          flush(consumer, writer, batch.drain());
          openedAt = System.nanoTime();
        }
      }

      // Persist any remaining bars during a graceful shutdown.
      if (!batch.isEmpty()) flush(consumer, writer, batch.drain());
    }
  }

  private static void flush(KafkaConsumer<String, byte[]> consumer, IcebergWriter writer, BoundedBatch.Batch batch) throws Exception {
    // Commit data to Iceberg before advancing Kafka offsets so failed writes remain retryable.
    int inserted = writer.append(batch.bars());

    // Advance each partition only past source records that are fully represented in this batch.
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (BoundedBatch.Entry entry : batch.entries()) {
      if (entry.finalBarOfSource()) {
        TopicPartition partition = new TopicPartition(entry.source().topic(), entry.source().partition());
        offsets.merge(partition, new OffsetAndMetadata(entry.source().offset() + 1),
            (left, right) -> left.offset() >= right.offset() ? left : right);
      }
    }
    if (!offsets.isEmpty()) consumer.commitSync(offsets);
    System.out.printf("committed_at=%s received=%d inserted=%d%n", java.time.Instant.now(), batch.entries().size(), inserted);
  }

  private static Duration elapsed(long openedAt) { return Duration.ofNanos(System.nanoTime() - openedAt); }
  private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
}
