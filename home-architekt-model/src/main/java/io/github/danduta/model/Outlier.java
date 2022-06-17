package io.github.danduta.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class Outlier {

    @JsonProperty("original")
    private final PotentialOutlier originalValue;
    @JsonProperty("stdevRation")
    private final double ratioToStandardDeviation;

    public Outlier(PotentialOutlier value, double ratioToStandardDeviation) {
        this.originalValue = value;
        this.ratioToStandardDeviation = ratioToStandardDeviation;
    }

    public UUID getId() {
        return originalValue.getId();
    }
}
