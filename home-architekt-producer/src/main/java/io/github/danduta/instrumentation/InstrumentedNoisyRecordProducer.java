package io.github.danduta.instrumentation;

import io.github.danduta.producer.NoisyRecordProducer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class InstrumentedNoisyRecordProducer extends NoisyRecordProducer {

    private static final Logger log = Logger.getLogger(InstrumentedNoisyRecordProducer.class);

    public static final AtomicInteger rps = new AtomicInteger(0);
    public static final Timer timer = new Timer();

    static {
        TimerTask tt = new TimerTask() {
            @Override
            public void run() {
                int oldRPS = rps.get();

                log.info("Current RPS:" + oldRPS);

                rps.set(0);
            }
        };

        timer.scheduleAtFixedRate(tt, 1000, 1000);
    }

    public static void stop() {
        timer.cancel();
    }

    public InstrumentedNoisyRecordProducer(Properties properties) {
        super(properties);
    }

    @Override
    public Void call() {
        Void returnValue = super.call();
        rps.incrementAndGet();

        return returnValue;
    }
}
