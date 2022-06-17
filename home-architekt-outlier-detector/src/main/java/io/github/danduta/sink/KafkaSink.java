package io.github.danduta.sink;

import io.github.danduta.app.SparkOutlierDetector;
import io.github.danduta.model.Outlier;
import io.github.danduta.serde.OutlierSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class KafkaSink<K, V> {
    protected static final String KAFKA_TOPIC = "outliers";
    protected KafkaProducer<K, V> producer;

    protected void lazyInitProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Map<String, Object> producerProps = new HashMap<>();

        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(SparkOutlierDetector.KAFKA_ENDPOINT));
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "Outlier Detector");

        producer = new KafkaProducer<>(producerProps, keySerializer, valueSerializer);
    }

    public abstract void send(K key, V value);
}
