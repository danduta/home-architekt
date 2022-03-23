package io.github.danduta.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.danduta.model.SensorRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.log4j.Logger;

public class SensorRecordDeserializer implements Deserializer<SensorRecord> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Logger logger = Logger.getLogger(this.getClass());

    @Override
    public SensorRecord deserialize(String topic, byte[] bytes) throws RecordDeserializationException {
        try {
            return mapper.readValue(bytes, SensorRecord.class);
        } catch (Exception e) {
            logger.error("Deserialization failed");
            throw new SerializationException("Failed to deserializer SensorRecord");
        }
    }
}
