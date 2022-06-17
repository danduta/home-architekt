package io.github.danduta.detector;

import org.apache.spark.streaming.api.java.JavaDStream;

public interface StreamProcessor<Input, Output> {

    JavaDStream<Output> process(JavaDStream<Input> input);
}
