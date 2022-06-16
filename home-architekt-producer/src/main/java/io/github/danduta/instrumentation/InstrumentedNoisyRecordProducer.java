package io.github.danduta.instrumentation;

import io.github.danduta.producer.NoisyRecordProducer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InstrumentedNoisyRecordProducer extends NoisyRecordProducer {

    private static final Logger log = Logger.getLogger(InstrumentedNoisyRecordProducer.class);

    public static final AtomicInteger rps = new AtomicInteger(0);
    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);

    private static NoisyRecordProducer instance;

    static {
        Runnable task = new TimerTask() {
            @Override
            public void run() {
                int oldRPS = rps.get();

                log.info("Current RPS:" + oldRPS);

                rps.set(0);
            }
        };

        executorService.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS);
    }

    private InstrumentedNoisyRecordProducer(Properties properties) {
        super(properties);
    }

    public static void init(Properties properties) {
        if (instance != null) {
            return;
        }

        instance = new InstrumentedNoisyRecordProducer(properties);
        log.info("Starting instrumentation");
    }

    public static NoisyRecordProducer getInstance() {
        if (instance == null) {
            throw new RuntimeException("Producer has not been initialized. Call NoisyRecordProducer.init().");
        }

        return instance;
    }

    public static void stop() {
        log.info("Stopping instrumentated producer");

        getInstance().flush();
        getInstance().close();

        executorService.shutdown();

        log.info("Stopped instrumentated producer");
    }

    @Override
    public void send(String topic, UUID producerId, double value, double scale) {
        super.send(topic, producerId, value, scale);
        rps.incrementAndGet();
    }
}
