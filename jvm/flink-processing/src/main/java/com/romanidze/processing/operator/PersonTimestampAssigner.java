package com.romanidze.processing.operator;

import com.romanidze.processing.domain.PersonModel;
import java.time.LocalDateTime;
import java.time.ZoneId;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class PersonTimestampAssigner implements AssignerWithPunctuatedWatermarks<PersonModel> {
  @Nullable
  @Override
  public Watermark checkAndGetNextWatermark(PersonModel personModel, long extractedTimestamp) {
    return new Watermark(extractedTimestamp - 15);
  }

  @Override
  public long extractTimestamp(PersonModel personModel, long l) {
    LocalDateTime ldt = LocalDateTime.now();

    return ldt.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
  }
}
