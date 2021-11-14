package com.romanidze.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.romanidze.processing.domain.PersonModel;
import com.romanidze.processing.operator.PersonAggregator;
import com.romanidze.processing.operator.PersonTimestampAssigner;
import com.romanidze.processing.schema.PersonModelDeserializationSchema;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Assert;
import org.junit.Test;

public class PersonAggregatorTest {

  private static class CollectingSink implements SinkFunction<Map<String, Long>> {

    public static List<Map<String, Long>> aggs = new ArrayList<>();

    @Override
    public synchronized void invoke(Map<String, Long> value, Context context) throws Exception {
      aggs.add(value);
    }
  }

  @Test
  public void testDeserialize() throws IOException {

    ObjectMapper objectMapper = new ObjectMapper();

    PersonModel personModel =
        new PersonModel(
            1,
            "test",
            "test",
            "test",
            "test",
            "test",
            "test",
            new Timestamp(System.currentTimeMillis()));

    byte[] messageSerialized = objectMapper.writeValueAsBytes(personModel);

    DeserializationSchema<PersonModel> deserializationSchema =
        new PersonModelDeserializationSchema();
    PersonModel messageDeserialized = deserializationSchema.deserialize(messageSerialized);

    Assert.assertEquals(personModel, messageDeserialized);
  }

  @Test
  public void testAggregate() throws Exception {
    PersonModel personModel1 =
        new PersonModel(
            1,
            "test",
            "test",
            "test",
            "test",
            "test",
            "test",
            new Timestamp(System.currentTimeMillis()));

    PersonModel personModel2 =
        new PersonModel(
            2,
            "test2",
            "test2",
            "test2",
            "test2",
            "test2",
            "test2",
            new Timestamp(System.currentTimeMillis()));

    PersonModel personModel3 =
        new PersonModel(
            3,
            "test3",
            "test3",
            "test3",
            "test3",
            "test3",
            "test3",
            new Timestamp(System.currentTimeMillis()));

    PersonModel personModel4 =
        new PersonModel(
            4,
            "test4",
            "test4",
            "test4",
            "test4",
            "test4",
            "test4",
            new Timestamp(System.currentTimeMillis()));

    PersonModel personModel5 =
        new PersonModel(
            5,
            "test5",
            "test5",
            "test5",
            "test5",
            "test5",
            "test5",
            new Timestamp(System.currentTimeMillis()));

    List<PersonModel> persons =
        List.of(personModel1, personModel2, personModel3, personModel4, personModel5);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    DataStreamSource<PersonModel> source = env.fromCollection(persons);

    CollectingSink sink = new CollectingSink();

    source
        .assignTimestampsAndWatermarks(new PersonTimestampAssigner())
        .timeWindowAll(Time.hours(24))
        .aggregate(new PersonAggregator())
        .addSink(sink);

    env.execute("persons-aggregate");

    Assert.assertEquals(sink.aggs.size(), 1);
  }
}
