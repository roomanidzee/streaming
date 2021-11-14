package com.romanidze.processing.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.romanidze.processing.domain.PersonModel;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PersonModelDeserializationSchema implements DeserializationSchema<PersonModel> {

  private static final ObjectMapper objectMapper =
      new ObjectMapper().registerModule(new JavaTimeModule());

  @Override
  public PersonModel deserialize(byte[] bytes) throws IOException {
    return objectMapper.readValue(bytes, PersonModel.class);
  }

  @Override
  public boolean isEndOfStream(PersonModel personModel) {
    return false;
  }

  @Override
  public TypeInformation<PersonModel> getProducedType() {
    return TypeInformation.of(PersonModel.class);
  }
}
