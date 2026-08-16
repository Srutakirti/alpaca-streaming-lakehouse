package io.gcehcatalog.loader;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** One long-lived Kafka consumer; it commits offsets only after an Iceberg commit. */
public final class Main {
  public static void main(String[] args) throws Exception {
    String broker = env("KAFKA_BROKER", "127.0.0.1:19092");
    String topic = env("KAFKA_TOPIC", "alpaca-bars");
    String warehouse = env("ICEBERG_WAREHOUSE", Path.of(".local-run", "warehouse").toAbsolutePath().toUri().toString());
    int maxRecords = Integer.parseInt(env("LOADER_MAX_RECORDS", "100"));
    Duration maxWait = Duration.ofSeconds(Long.parseLong(env("LOADER_MAX_SECONDS", "300")));
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, env("KAFKA_GROUP_ID", "alpaca-iceberg-loader"));
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    AtomicBoolean running = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "loader-shutdown"));
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
         HadoopIcebergWriter writer = new HadoopIcebergWriter(warehouse)) {
      consumer.subscribe(List.of(topic));
      List<AlpacaBar> batch = new ArrayList<>();
      long openedAt = System.nanoTime();
      while (running.get()) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
        for (var record : records) batch.addAll(AlpacaBar.decodeFrame(record.value()));
        if (!batch.isEmpty() && (batch.size() >= maxRecords || elapsed(openedAt).compareTo(maxWait) >= 0)) {
          flush(consumer, writer, batch);
          batch.clear();
          openedAt = System.nanoTime();
        }
      }
      if (!batch.isEmpty()) flush(consumer, writer, batch);
    }
  }

  private static void flush(KafkaConsumer<String, byte[]> consumer, HadoopIcebergWriter writer, List<AlpacaBar> batch) throws Exception {
    int inserted = writer.append(batch);
    consumer.commitSync();
    System.out.printf("committed_at=%s received=%d inserted=%d%n", java.time.Instant.now(), batch.size(), inserted);
  }

  private static Duration elapsed(long openedAt) { return Duration.ofNanos(System.nanoTime() - openedAt); }
  private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
}
