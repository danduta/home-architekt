package io.github.danduta.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

@Builder
@Getter
@AllArgsConstructor
@ToString
public class SensorRecord implements PotentialOutlier, Serializable {

    @JsonFormat(shape= JsonFormat.Shape.NUMBER_INT, pattern="s")
    @JsonProperty("timestamp")
    private final Timestamp timestamp;
    @JsonProperty("value")
    private final double value;
    @JsonProperty("id")
    private final UUID id;
    @JsonProperty("producerId")
    private final UUID producerId;

    public SensorRecord() {
        timestamp = null;
        value = 0.0;
        id = null;
        producerId = null;
    }

}
