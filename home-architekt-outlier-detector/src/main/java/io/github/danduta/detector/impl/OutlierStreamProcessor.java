package io.github.danduta.detector.impl;

import io.github.danduta.detector.StreamProcessor;
import io.github.danduta.model.Outlier;
import io.github.danduta.model.PotentialOutlier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.util.UUID;

public final class OutlierStreamProcessor implements StreamProcessor<ConsumerRecord<UUID, PotentialOutlier>, Outlier> {

    @Override
    public JavaDStream<Outlier> process(JavaDStream<ConsumerRecord<UUID, PotentialOutlier>> input) {
        return input
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .window(Durations.milliseconds(1000), Durations.milliseconds(1000))
                .mapWithState(getMappingFunction())
                .filter(Tuple2::_2)
                .map(Tuple2::_1);
    }

    private static StateSpec<UUID, PotentialOutlier, ?, Tuple2<Outlier, Boolean>> getMappingFunction() {
        return StatefulOutlierMappingFunction.build().getSpec();
    }
}
