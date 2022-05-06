package io.github.danduta.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Builder
@Getter
public class SensorRecord implements Serializable {

    @JsonFormat(shape= JsonFormat.Shape.NUMBER_INT, pattern="s")
    @JsonProperty("timestamp")
    private final Timestamp timestamp;
    @JsonProperty("value")
    private final Double value;
    @JsonProperty("id")
    private final UUID id;
    @JsonProperty("producerId")
    private final UUID producerId;
}
