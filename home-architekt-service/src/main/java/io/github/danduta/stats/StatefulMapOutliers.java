package io.github.danduta.stats;

import io.github.danduta.model.SensorRecord;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import scala.Tuple2;
import scala.Tuple3;

import java.util.UUID;

public final class StatefulMapOutliers implements Function3<UUID, Optional<SensorRecord>, State<Tuple3<Long, Double, Double>>, Tuple2<UUID, Boolean>> {

    private StatefulMapOutliers() {}

    @Override
    public Tuple2<UUID, Boolean> call(UUID producerId, Optional<SensorRecord> sensorRecordOptional, State<Tuple3<Long, Double, Double>> state) throws Exception {
        if (!sensorRecordOptional.isPresent()) {
            return null;
        }

        SensorRecord record = sensorRecordOptional.get();
        double currentValue = record.getValue();

        if (!state.exists()) {
            state.update(Tuple3.apply(1L, currentValue, 0.0));
            return Tuple2.apply(record.getId(), false);
        }

        Tuple3<Long, Double, Double> recordState = state.get();

        long oldCount = recordState._1();
        double oldMean = recordState._2();
        double oldVariance = recordState._3();

        double stdev = Math.sqrt(oldVariance / oldCount);
        boolean isOutlier = Math.abs(currentValue - oldMean) >= 4 * stdev;

        long newCount = oldCount + 1;
        double newMean = oldMean + (currentValue - oldMean) / newCount;
        double newVariance = oldVariance + (currentValue - oldMean) * (currentValue - newMean);

        state.update(Tuple3.apply(newCount, newMean, newVariance));

        return Tuple2.apply(record.getId(), isOutlier);
    }

    public static StatefulMapOutliers build() {
        return new StatefulMapOutliers();
    }

    public StateSpec<UUID, SensorRecord, Tuple3<Long, Double, Double>, Tuple2<UUID, Boolean>> getSpec() {
        return StateSpec.function(this);
    }
}
