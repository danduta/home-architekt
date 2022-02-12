package io.github.danduta.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SensorProducerSimulator {

    private static final String DATASET_CSV_PATH = "DATASET_CSV_PATH";
    private static final String KAFKA_TOPIC = "KAFKA_TOPIC";

    private static final Properties props = new Properties();
    private static final Logger log = Logger.getLogger(SensorProducerSimulator.class);

    static {
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            log.error("Error while loading properties from jar", e);
        }
    }

    public static void main(String[] args) {
        KafkaProducer<Long, Double> producer = new KafkaProducer<>(props);

        try {
            FileReader in = new FileReader(System.getenv(DATASET_CSV_PATH));
            CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

            log.info("Parsing started");
            log.info("Dataset header: " + records.getHeaderMap());

            String topic = System.getenv(KAFKA_TOPIC);

            for (CSVRecord record : records) {
                ProducerRecord<Long, Double> producerRecord = new ProducerRecord<>(topic,
                    Long.valueOf(record.get("time")),
                    Double.valueOf(record.get("House overall [kW]")));

                producer.send(producerRecord);
                log.info(producerRecord);
            }
        } catch (FileNotFoundException e) {
            log.error("Dataset could not be found", e);
        } catch (IOException e) {
            log.error("Exception occurred while reading the dataset", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
