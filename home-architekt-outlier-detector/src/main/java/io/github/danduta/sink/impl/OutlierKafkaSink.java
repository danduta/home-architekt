package io.github.danduta.sink.impl;

import io.github.danduta.model.Outlier;
import io.github.danduta.serde.OutlierSerializer;
import io.github.danduta.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.io.Serializable;
import java.util.UUID;

public class OutlierKafkaSink extends KafkaSink<UUID, Outlier> implements Serializable {

    @Override
    public void send(UUID id, Outlier outlier) {
        if (producer == null) {
            lazyInitProducer(new UUIDSerializer(), new OutlierSerializer());
        }

        producer.send(new ProducerRecord<>(KAFKA_TOPIC, outlier.getId(), outlier));
    }
}
