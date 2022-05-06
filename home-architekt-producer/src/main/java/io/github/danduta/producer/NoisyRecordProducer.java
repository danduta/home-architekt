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
import java.util.concurrent.Callable;

public class NoisyRecordProducer extends KafkaProducer<UUID, SensorRecord> implements Callable<Void> {

    private static final Random RANDOM = new Random();
    private final double scale = RANDOM.nextDouble() * 10.0 + 1.0;
    private final UUID producerId = UUID.randomUUID();

    private double currentValue;
    private String currentTopic;

    public NoisyRecordProducer(Properties properties) {
        super(properties, new UUIDSerializer(), new SensorRecordSerializer());
    }

    public void setCurrent(double value, String topic) {
        currentValue = value;
        currentTopic = topic;
    }

    @Override
    public Void call() {

        final double value = currentValue * scale + RANDOM.nextGaussian();
        final SensorRecord record = SensorRecord.builder()
                .timestamp(Timestamp.from(Instant.now()))
                .value(value)
                .id(UUID.randomUUID())
                .producerId(producerId)
                .build();

        ProducerRecord<UUID, SensorRecord> producerRecord = new ProducerRecord<>(
                currentTopic,
                producerId,
                record);

        send(producerRecord);
        return null;
    }
}

