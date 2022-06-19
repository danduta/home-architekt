package io.github.danduta.app;

import io.github.danduta.detector.StreamProcessor;
import io.github.danduta.detector.impl.OutlierStreamProcessor;
import io.github.danduta.model.Outlier;
import io.github.danduta.model.PotentialOutlier;
import io.github.danduta.serde.SensorRecordDeserializer;
import io.github.danduta.sink.impl.OutlierKafkaSink;
import io.github.danduta.sink.impl.OutlierKafkaStreamSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkOutlierDetector {
    public static final String KAFKA_ENDPOINT = "KAFKA_ENDPOINT";

    public static final Logger logger = LoggerFactory.getLogger(SparkOutlierDetector.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(SparkOutlierDetector.class.getName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.streaming.backpressure.enabled", "true");
        conf.set("spark.streaming.backpressure.initialRate", "100000");
        conf.set("spark.stream.backpressure.maxRate", "200000");
        conf.registerKryoClasses(new Class[]{ConsumerRecord.class});
        conf.setExecutorEnv(KAFKA_ENDPOINT, System.getenv(KAFKA_ENDPOINT));

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint("/tmp/cp");
        jssc.sparkContext().setLogLevel("WARN");

        Broadcast<OutlierKafkaSink> sink = jssc.sparkContext().broadcast(new OutlierKafkaSink());

        org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        JavaInputDStream<ConsumerRecord<UUID, PotentialOutlier>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList("use"), getKafkaConsumerProperties()));

        StreamProcessor<ConsumerRecord<UUID, PotentialOutlier>, Outlier> processor = new OutlierStreamProcessor();
        OutlierKafkaStreamSink streamSink = new OutlierKafkaStreamSink();

        JavaDStream<Outlier> outlierStream = processor.process(stream);
        streamSink.persistInSink(outlierStream, sink.getValue());

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Execution was interrupted", e);
        }
    }

    private static Map<String, Object> getKafkaConsumerProperties() {
        Map<String, Object> kafkaConsumerParams = new HashMap<>();
        kafkaConsumerParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KAFKA_ENDPOINT));
        kafkaConsumerParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        kafkaConsumerParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorRecordDeserializer.class);
        kafkaConsumerParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumerParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        kafkaConsumerParams.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return kafkaConsumerParams;
    }
}
