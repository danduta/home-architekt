package io.github.danduta.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Application {

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(Application.class.getName());
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        logger.info("Started Spark application.");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "broker:9092");
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", DoubleDeserializer.class);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("hub-data");

        JavaInputDStream<ConsumerRecord<Long, Double>> stream =
            KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<Long, Double>Subscribe(topics, kafkaParams)
            );

        stream.foreachRDD((v1, v2) -> {
            v1.foreach(record -> logger.info("{}: {} at {}", record.topic(), record.key(), record.value()));
        });

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Execution was interrupted", e);
        }
    }
}
