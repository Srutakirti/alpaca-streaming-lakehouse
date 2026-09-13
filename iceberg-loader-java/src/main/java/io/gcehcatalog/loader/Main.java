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
    Duration heartbeatInterval = Duration.ofSeconds(
        Long.parseLong(env("LOADER_HEARTBEAT_SECONDS", "60")));
    Duration stallThreshold = Duration.ofSeconds(
        Long.parseLong(env("LOADER_STALL_SECONDS", "900")));
    int maxPollIntervalMs = Integer.parseInt(env("LOADER_MAX_POLL_INTERVAL_MS", "900000"));
    LoaderHealthReporter health = new LoaderHealthReporter(
        topic, heartbeatInterval, stallThreshold);
    health.starting();

    // Configure an at-least-once Kafka consumer whose offsets are committed explicitly.
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, env("KAFKA_GROUP_ID", "alpaca-iceberg-loader"));
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    // Coordinate graceful termination so pending data can be flushed before resources close.
    AtomicBoolean running = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "loader-shutdown"));

    // Keep one Kafka consumer and one Iceberg writer open for the lifetime of the process.
    try {
      try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
           IcebergWriter writer = new IcebergWriter(catalogConfig)) {
        consumer.subscribe(List.of(topic));
        BoundedBatch batch = new BoundedBatch(maxRecords);
        BatchWindow batchWindow = new BatchWindow();

        // Decode Kafka frames into bars and flush each batch as soon as it reaches its size limit.
        while (running.get()) {
          ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
          for (var record : records) {
            health.recordInput();
            boolean wasEmpty = batch.isEmpty();
            BoundedBatch.Source source = new BoundedBatch.Source(
                record.topic(), record.partition(), record.offset());
            List<AlpacaBar> bars = AlpacaBar.decodeFrame(record.value());
            if (wasEmpty && !bars.isEmpty()) batchWindow.open();
            List<BoundedBatch.Batch> fullBatches = batch.add(source, bars);
            for (BoundedBatch.Batch fullBatch : fullBatches) {
              flush(consumer, writer, fullBatch, health);
            }
            if (!fullBatches.isEmpty()) {
              batchWindow.reset(!batch.isEmpty());
            }
          }

          // Flush a partial batch after its own time limit, excluding time spent idle.
          if (!batch.isEmpty() && batchWindow.expired(maxWait)) {
            flush(consumer, writer, batch.drain(), health);
            batchWindow.close();
          }
          health.maybeHeartbeat(consumer, batch);
        }

        // Persist any remaining bars during a graceful shutdown.
        if (!batch.isEmpty()) flush(consumer, writer, batch.drain(), health);
      }
    } catch (LoaderCommitException failure) {
      throw failure;
    } catch (Exception failure) {
      health.loaderFailed(failure);
      throw failure;
    }
  }

  private static void flush(
      KafkaConsumer<String, byte[]> consumer,
      IcebergWriter writer,
      BoundedBatch.Batch batch,
      LoaderHealthReporter health) throws Exception {
    health.commitStarted(batch);
    long startedAt = System.nanoTime();
    try {
      // Commit data to Iceberg before advancing Kafka offsets so failed writes remain retryable.
      int inserted = writer.append(batch.bars());

      // Advance each partition only past source records that are fully represented in this batch.
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (BoundedBatch.Entry entry : batch.entries()) {
        if (entry.finalBarOfSource()) {
          TopicPartition partition = new TopicPartition(
              entry.source().topic(), entry.source().partition());
          offsets.merge(partition, new OffsetAndMetadata(entry.source().offset() + 1),
              (left, right) -> left.offset() >= right.offset() ? left : right);
        }
      }
      if (!offsets.isEmpty()) {
        try {
          consumer.commitSync(offsets);
        } catch (Exception error) {
          throw new LoaderCommitException(
              LoaderCommitException.Type.OFFSET_COMMIT_FAILED,
              "committed",
              "not_committed",
              error);
        }
      }
      Duration duration = elapsed(startedAt);
      health.commitSucceeded(duration);
      System.out.printf("committed_at=%s received=%d inserted=%d%n",
          java.time.Instant.now(), batch.entries().size(), inserted);
    } catch (LoaderCommitException failure) {
      health.commitFailed(failure, elapsed(startedAt));
      throw failure;
    }
  }

  private static Duration elapsed(long openedAt) { return Duration.ofNanos(System.nanoTime() - openedAt); }
  private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
}
