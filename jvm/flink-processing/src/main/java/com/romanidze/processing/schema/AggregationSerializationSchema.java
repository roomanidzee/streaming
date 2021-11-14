package com.romanidze.processing.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class AggregationSerializationSchema implements SerializationSchema<Map<String, Long>> {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public byte[] serialize(Map<String, Long> stringLongMap) {

    try {
      String json = objectMapper.writeValueAsString(stringLongMap);
      return json.getBytes(StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      return new byte[0];
    }
  }
}
