package io.github.danduta.producer;

import io.github.danduta.instrumentation.InstrumentedNoisyRecordProducer;

public class NoisyRecordCallback implements Runnable {

    private final NoisyRecordProducerPattern noisePattern;
    private final String topic;
    private final double value;

    public NoisyRecordCallback(String topic, double value, NoisyRecordProducerPattern noisePattern) {
        this.topic = topic;
        this.value = value;
        this.noisePattern = noisePattern;
    }

    @Override
    public void run() {
        InstrumentedNoisyRecordProducer.getInstance().send(
                topic,
                noisePattern.getProducerId(),
                value,
                noisePattern.getScale());
    }
}
