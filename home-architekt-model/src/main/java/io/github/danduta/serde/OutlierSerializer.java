package io.github.danduta.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.danduta.model.Outlier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

public class OutlierSerializer implements Serializer<Outlier> {

    private final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
    private final Logger logger = Logger.getLogger(this.getClass());

    @Override
    public byte[] serialize(String s, Outlier outlier) {
        try {
            return mapper.valueToTree(outlier).binaryValue();
        } catch (Exception e) {
            logger.error("Serialization failed", e);
            throw new SerializationException("Failed to serialize SensorRecord");
        }
    }
}
