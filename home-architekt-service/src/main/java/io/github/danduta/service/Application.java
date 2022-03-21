package io.github.danduta.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
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

import java.util.*;

public class Application {

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Application.class.getName());
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        logger.info("Started Spark application.");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka-broker:9092");
        kafkaParams.put("key.deserializer", UUIDDeserializer.class);
        kafkaParams.put("value.deserializer", DoubleDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group_id");

        Collection<String> topics = Collections.singletonList("use");

        JavaInputDStream<ConsumerRecord<UUID, Double>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<UUID, Double>Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD((consumerRecordJavaRDD, time) -> {
            if (consumerRecordJavaRDD.isEmpty())
                return;

            consumerRecordJavaRDD.foreachPartition(consumerRecordIterator -> {
                if (consumerRecordIterator.hasNext())
                    logger.info(String.valueOf(consumerRecordIterator.next().value()));
            });
        });


        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Execution was interrupted", e);
        }
    }
}
