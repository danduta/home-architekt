package io.github.danduta.producer;

import io.github.danduta.model.SensorRecord;
import io.github.danduta.serde.SensorRecordSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class NoisyRecordProducer extends KafkaProducer<UUID, SensorRecord> {

    private static final Random RANDOM = new Random();


    protected NoisyRecordProducer(Properties properties) {
        super(properties, new UUIDSerializer(), new SensorRecordSerializer());
    }

    public void send(String topic, UUID producerId, double value, double scale) {
        value = value * scale + RANDOM.nextGaussian();

        final SensorRecord record = SensorRecord.builder()
                .timestamp(Timestamp.from(Instant.now()))
                .value(value)
                .id(UUID.randomUUID())
                .producerId(producerId)
                .build();

        ProducerRecord<UUID, SensorRecord> producerRecord = new ProducerRecord<>(
                topic,
                producerId,
                record);

        send(producerRecord);
    }
}

