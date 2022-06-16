package io.github.danduta.producer;

import java.util.Random;
import java.util.UUID;

public class NoisyRecordProducerPattern {

    private static final Random RANDOM = new Random();

    private final double scale = RANDOM.nextDouble() * 10.0 + 1.0;
    private final UUID producerId;

    public NoisyRecordProducerPattern() {
        producerId = UUID.randomUUID();
    }

    public double getScale() {
        return scale;
    }

    public UUID getProducerId() {
        return producerId;
    }
}
