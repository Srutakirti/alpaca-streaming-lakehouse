package io.gcehcatalog.loader;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/** Creates the pipeline topic once without requiring a Python runtime on the VM. */
public final class TopicProvisioner {
  private TopicProvisioner() {}

  public static void main(String[] args) throws Exception {
    String broker = env("KAFKA_BROKER", "127.0.0.1:19092");
    String topic = env("KAFKA_TOPIC", "alpaca-bars");
    Properties properties = new Properties();
    properties.put("bootstrap.servers", broker);
    try (AdminClient client = AdminClient.create(properties)) {
      try {
        client.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
      } catch (ExecutionException error) {
        if (!(error.getCause() instanceof TopicExistsException)) throw error;
      }
    }
    System.out.printf("topic_ready=%s%n", topic);
  }

  private static String env(String name, String fallback) {
    return System.getenv().getOrDefault(name, fallback);
  }
}
