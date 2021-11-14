package com.romanidze.processing.connector;

import com.romanidze.processing.domain.PersonModel;
import com.romanidze.processing.schema.AggregationSerializationSchema;
import com.romanidze.processing.schema.PersonModelDeserializationSchema;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class PersonModelConnector {

  public static FlinkKafkaConsumer<PersonModel> createConsumer(
      String topic, String kafkaAddress, String kafkaGroup) {

    Properties props = new Properties();

    props.setProperty("bootstrap.servers", kafkaAddress);
    props.setProperty("group.id", kafkaGroup);

    return new FlinkKafkaConsumer<>(topic, new PersonModelDeserializationSchema(), props);
  }

  public static FlinkKafkaProducer<Map<String, Long>> createProducer(
      String topic, String kafkaAddress) {
    return new FlinkKafkaProducer<>(kafkaAddress, topic, new AggregationSerializationSchema());
  }
}
