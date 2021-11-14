package com.romanidze.processing.operator;

import com.romanidze.processing.domain.PersonModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

public class PersonAggregator
    implements AggregateFunction<PersonModel, List<PersonModel>, Map<String, Long>> {
  @Override
  public List<PersonModel> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<PersonModel> add(PersonModel personModel, List<PersonModel> personModels) {
    personModels.add(personModel);
    return personModels;
  }

  @Override
  public Map<String, Long> getResult(List<PersonModel> personModels) {

    Map<String, Long> resultMap = new HashMap<>();

    personModels.forEach(
        elem -> {
          if (resultMap.get(elem.getProfession()) == null) {
            resultMap.put(elem.getProfession(), 1L);
          } else {
            resultMap.put(elem.getProfession(), resultMap.get(elem.getProfession()) + 1);
          }
        });

    return resultMap;
  }

  @Override
  public List<PersonModel> merge(List<PersonModel> personModels, List<PersonModel> acc1) {
    personModels.addAll(acc1);
    return personModels;
  }
}
