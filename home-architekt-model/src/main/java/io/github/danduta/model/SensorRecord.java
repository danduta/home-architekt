package io.github.danduta.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;

public class SensorRecord implements Serializable {

    @JsonFormat(shape= JsonFormat.Shape.NUMBER_INT, pattern="s")
    @JsonProperty("timestamp")
    private final Timestamp timestamp;
    @JsonProperty("value")
    private final Double value;

    public SensorRecord() {
        timestamp = null;
        value = null;
    }

    public SensorRecord(Timestamp timestamp, Double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public SensorRecord(long time, Double value) {
        this.timestamp = Timestamp.from(Instant.ofEpochMilli(time));
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Double getValue() {
        return value;
    }
}
