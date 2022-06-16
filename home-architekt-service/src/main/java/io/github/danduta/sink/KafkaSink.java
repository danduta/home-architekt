package io.github.danduta.sink;

import io.github.danduta.model.SensorRecord;
import io.github.danduta.serde.SensorRecordSerializer;
import io.github.danduta.service.Application;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaSink implements Serializable {

    private static final String KAFKA_TOPIC = "outliers";
    private KafkaProducer<UUID, SensorRecord> producer;

    public void send(SensorRecord record) {
        if (producer == null) {
            Map<String, Object> producerProps = new HashMap<>();

            producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(Application.KAFKA_ENDPOINT));
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "Outlier Detector");

            producer = new KafkaProducer<>(producerProps, new UUIDSerializer(), new SensorRecordSerializer());
        }


        producer.send(new ProducerRecord<>(KAFKA_TOPIC, record.getProducerId(), record));
    }
}
