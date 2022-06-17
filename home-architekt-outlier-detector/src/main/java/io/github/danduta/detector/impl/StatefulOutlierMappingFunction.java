package io.github.danduta.detector.impl;

import io.github.danduta.model.Outlier;
import io.github.danduta.model.PotentialOutlier;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import scala.Tuple2;

import java.util.UUID;

public final class StatefulOutlierMappingFunction implements Function3<UUID, Optional<PotentialOutlier>, State<StatefulOutlierMappingFunction.OutlierDetectionState>, Tuple2<Outlier, Boolean>> {

    private StatefulOutlierMappingFunction() {
    }

    protected static class OutlierDetectionState {

        long count = 1;
        double mean = 0.0;
        double variance = 0.0;

        public OutlierDetectionState(OutlierDetectionState oldState, double currentValue) {
            count = oldState.count + 1;
            mean = oldState.mean + (currentValue - oldState.mean) / count;
            variance = oldState.variance + (currentValue - oldState.mean) * (currentValue - mean);
        }

        public OutlierDetectionState() {
        }

        public double getStdevRatioFor(double value) {
            return Math.abs(value - mean) / Math.sqrt(variance / count);
        }
    }

    @Override
    public Tuple2<Outlier, Boolean> call(
            UUID key,
            Optional<PotentialOutlier> potentialOutlierOptional,
            State<OutlierDetectionState> state) throws Exception {
        if (!potentialOutlierOptional.isPresent()) {
            return null;
        }

        PotentialOutlier record = potentialOutlierOptional.get();
        double currentValue = record.getValue();

        if (!state.exists()) {
            state.update(new OutlierDetectionState());
            return Tuple2.apply(new Outlier(record, 0.0), false);
        }

        OutlierDetectionState oldState = state.get();
        state.update(new OutlierDetectionState(oldState, currentValue));

        double stdevRatio = oldState.getStdevRatioFor(currentValue);

        return Tuple2.apply(new Outlier(record, stdevRatio), stdevRatio >= 4);
    }

    public static StatefulOutlierMappingFunction build() {
        return new StatefulOutlierMappingFunction();
    }

    public StateSpec<UUID, PotentialOutlier, OutlierDetectionState, Tuple2<Outlier, Boolean>> getSpec() {
        return StateSpec.function(this);
    }
}
