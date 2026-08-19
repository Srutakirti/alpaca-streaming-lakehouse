package io.gcehcatalog.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Publishes a bounded, deterministic Alpaca-compatible batch without Python on the VM. */
public final class SyntheticPublisher {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Instant START = Instant.parse("2026-01-02T14:30:00Z");

  private SyntheticPublisher() {}

  public static void main(String[] args) throws Exception {
    String broker = env("KAFKA_BROKER", "127.0.0.1:19092");
    String topic = env("KAFKA_TOPIC", "alpaca-bars");
    int periods = Integer.parseInt(env("SYNTHETIC_PERIODS", "6"));
    String[] symbols = env("SYNTHETIC_SYMBOLS", "AAPL,MSFT").split(",");
    if (periods < 1) throw new IllegalArgumentException("SYNTHETIC_PERIODS must be positive");

    Properties properties = new Properties();
    properties.put("bootstrap.servers", broker);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", ByteArraySerializer.class.getName());
    properties.put("acks", "all");
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
      for (int period = 0; period < periods; period++) {
        Instant eventTime = START.plus(period, ChronoUnit.MINUTES);
        byte[] frame = JSON.writeValueAsBytes(frame(eventTime, period, symbols));
        producer.send(new ProducerRecord<>(topic, eventTime.toString(), frame)).get();
      }
    }
    System.out.printf("synthetic_published periods=%d symbols=%d topic=%s%n", periods, symbols.length, topic);
  }

  static List<AlpacaBar> frame(Instant eventTime, int period, String[] symbols) {
    List<AlpacaBar> bars = new ArrayList<>();
    for (int index = 0; index < symbols.length; index++) {
      String symbol = symbols[index].trim();
      if (symbol.isEmpty()) throw new IllegalArgumentException("SYNTHETIC_SYMBOLS must not contain an empty symbol");
      double base = switch (symbol) { case "AAPL" -> 175.0; case "MSFT" -> 380.0; default -> 100.0 + index; };
      double close = round(base + period * 0.25 + index * 0.1);
      double open = round(close - 0.05);
      double high = round(close + 0.15);
      double low = round(open - 0.1);
      long volume = 1_000L + period * 10L + index;
      bars.add(new AlpacaBar("b", symbol, open, high, low, close, volume, eventTime.toString(), Math.max(1, volume / 100), round((high + low + close) / 3)));
    }
    return bars;
  }

  private static double round(double value) { return Math.round(value * 100.0) / 100.0; }

  private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
}
