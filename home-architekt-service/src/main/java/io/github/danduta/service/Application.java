package io.github.danduta.service;

import io.github.danduta.model.SensorRecord;
import io.github.danduta.serde.SensorRecordDeserializer;
import io.github.danduta.stats.StatefulMapOutliers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class Application {

    private static final String KAFKA_ENDPOINT = "KAFKA_ENDPOINT";

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Application.class.getName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.streaming.backpressure.enabled", "true");
        conf.set("spark.streaming.receiver.maxRate", "5000");
        conf.set("spark.streaming.backpressure.initialRate", "5000");
        conf.registerKryoClasses(new Class[]{ConsumerRecord.class});

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint("/tmp");

        org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KAFKA_ENDPOINT));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorRecordDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        kafkaParams.put("startingOffsets", "earliest");

        Collection<String> topics = Collections.singletonList("use");

        JavaInputDStream<ConsumerRecord<UUID, SensorRecord>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        StatefulMapOutliers mappingFunction = StatefulMapOutliers.build();

        stream
                .window(Durations.milliseconds(1000), Durations.milliseconds(1000))
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .mapWithState(mappingFunction.getSpec())
                .filter(Tuple2::_2)
                .count()
                .print();

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Execution was interrupted", e);
        }
    }
}
