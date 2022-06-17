package io.github.danduta.sink.impl;

import io.github.danduta.model.Outlier;
import io.github.danduta.sink.KafkaSink;
import io.github.danduta.sink.KafkaStreamSink;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.UUID;

public class OutlierKafkaStreamSink implements KafkaStreamSink<UUID, Outlier> {
    @Override
    public void persistInSink(JavaDStream<Outlier> stream, KafkaSink<UUID, Outlier> sink) {
        stream.foreachRDD(rdd -> rdd.foreachPartitionAsync(iterator -> {
            while (iterator.hasNext()) {
                Outlier value = iterator.next();
                sink.send(value.getId(), value);
            }
        }));
    }
}
