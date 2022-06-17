package io.github.danduta.service;

import io.github.danduta.model.SensorRecord;
import io.github.danduta.serde.SensorRecordDeserializer;
import io.github.danduta.sink.KafkaSink;
import io.github.danduta.stats.StatefulMapOutliers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Application {
    public static final String KAFKA_ENDPOINT = "KAFKA_ENDPOINT";

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Application.class.getName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.streaming.backpressure.enabled", "true");
        conf.set("spark.streaming.backpressure.initialRate", "100000");
        conf.registerKryoClasses(new Class[]{ConsumerRecord.class});
        conf.setExecutorEnv(KAFKA_ENDPOINT, System.getenv(KAFKA_ENDPOINT));

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint("/tmp/cp");
        jssc.sparkContext().setLogLevel("WARN");

        Broadcast<KafkaSink> sink = jssc.sparkContext().broadcast(new KafkaSink());

        org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        JavaInputDStream<ConsumerRecord<UUID, SensorRecord>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singletonList("use"), getKafkaConsumerProperties())
                );

        StatefulMapOutliers mappingFunction = StatefulMapOutliers.build();
        stream
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .window(Durations.milliseconds(1000), Durations.milliseconds(1000))
                .mapWithState(mappingFunction.getSpec())
                .filter(Tuple2::_2)
                .foreachRDD(rdd -> rdd.foreachPartitionAsync(iterator -> {
                    while (iterator.hasNext()) {
                        SensorRecord record = iterator.next()._1;
                        sink.value().send(record);
                    }
                }));

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
        kafkaConsumerParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConsumerParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return kafkaConsumerParams;
    }
}
