package io.github.danduta.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.danduta.model.SensorRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

public class SensorRecordSerializer implements Serializer<SensorRecord> {

    private final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
    private final Logger logger = Logger.getLogger(this.getClass());

    @Override
    public byte[] serialize(String topic, SensorRecord sensorRecord) {
        try {
            if (sensorRecord.getTimestamp() == null) {
                return null;
            }

            return mapper.writeValueAsBytes(sensorRecord);
        } catch (Exception e) {
            logger.error("Serialization failed", e);
            throw new SerializationException("Failed to serialize SensorRecord");
        }
    }
}
