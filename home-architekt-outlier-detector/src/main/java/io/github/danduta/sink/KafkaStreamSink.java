package io.github.danduta.sink;

import org.apache.spark.streaming.api.java.JavaDStream;

public interface KafkaStreamSink<K, V> {

    void persistInSink(JavaDStream<V> stream, KafkaSink<K, V> sink);
}
