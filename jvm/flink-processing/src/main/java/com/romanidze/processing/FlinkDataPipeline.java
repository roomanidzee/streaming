package com.romanidze.processing;

import com.romanidze.processing.connector.PersonModelConnector;
import com.romanidze.processing.domain.PersonModel;
import com.romanidze.processing.operator.PersonAggregator;
import com.romanidze.processing.operator.PersonTimestampAssigner;
import java.util.Map;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class FlinkDataPipeline {

  public static void main(String[] args) throws Exception {

    String inputTopic = "persons-json";
    String outputTopic = "aggregation-result";
    String consumerGroup = "aggregation-group";
    String kafkaAddress = "localhost:9093";

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    FlinkKafkaConsumer<PersonModel> consumer =
        PersonModelConnector.createConsumer(inputTopic, kafkaAddress, consumerGroup);

    consumer.setStartFromEarliest();
    consumer.assignTimestampsAndWatermarks(new PersonTimestampAssigner());

    FlinkKafkaProducer<Map<String, Long>> producer =
        PersonModelConnector.createProducer(outputTopic, kafkaAddress);

    DataStream<PersonModel> personsStream = environment.addSource(consumer);

    personsStream.timeWindowAll(Time.hours(24)).aggregate(new PersonAggregator()).addSink(producer);

    environment.execute("persons-processing");
  }
}
