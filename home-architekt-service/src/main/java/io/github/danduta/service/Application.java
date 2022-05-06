package io.github.danduta.service;

import io.github.danduta.model.SensorRecord;
import io.github.danduta.serde.SensorRecordDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class Application {

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Application.class.getName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{ConsumerRecord.class});
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        logger.info("Started Spark application.");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181,localhost:9092");
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

        stream.
                window(Durations.milliseconds(1000), Durations.milliseconds(1000)).
                foreachRDD(rdd -> rdd
                        .sortBy(record -> record.value().getTimestamp(), true, 1)
                        .groupBy(ConsumerRecord::key).map(Tuple2::_2)
                        .foreach(iterable -> {
                            Iterator<ConsumerRecord<UUID, SensorRecord>> iterator = iterable.iterator();
                            if (iterator.hasNext()) {
                                String producerId = iterator.next().key().toString();
                                logger.info("Starting group processing for producer: " + producerId);
                            }

                            iterable.forEach(record -> logger.info(record.value().toString()));
                        }));

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Execution was interrupted", e);
        }
    }
}
