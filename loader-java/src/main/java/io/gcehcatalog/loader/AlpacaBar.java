package io.gcehcatalog.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Exact wire fields published by Alpaca's bar WebSocket messages. */
public record AlpacaBar(
    String T, String S, double o, double h, double l, double c, long v, String t, long n, double vw) {
  private static final ObjectMapper JSON = new ObjectMapper();

  public AlpacaBar {
    if (!"b".equals(T) || S == null || S.isBlank()) throw new IllegalArgumentException("expected Alpaca bar fields T=b and S");
    Instant.parse(t); // RFC3339/UTC is required at the boundary.
  }

  public static List<AlpacaBar> decodeFrame(byte[] payload) throws Exception {
    JsonNode root = JSON.readTree(payload);
    if (!root.isArray()) throw new IllegalArgumentException("Alpaca frame must be a JSON array");
    List<AlpacaBar> bars = new ArrayList<>();
    for (JsonNode node : root) {
      bars.add(new AlpacaBar(
          node.required("T").asText(), node.required("S").asText(), node.required("o").asDouble(),
          node.required("h").asDouble(), node.required("l").asDouble(), node.required("c").asDouble(),
          node.required("v").asLong(), node.required("t").asText(), node.required("n").asLong(),
          node.required("vw").asDouble()));
    }
    return bars;
  }
}
