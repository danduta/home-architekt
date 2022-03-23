package io.github.danduta.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;
import java.time.Instant;

public class SensorRecord {

    @JsonProperty("timestamp")
    private final Timestamp timestamp;
    @JsonProperty("value")
    private final Object value;

    public SensorRecord(long time, Object value) {
        this.timestamp = Timestamp.from(Instant.ofEpochMilli(time));
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }
}
