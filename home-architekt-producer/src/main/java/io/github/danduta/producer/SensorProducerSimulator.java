package io.github.danduta.producer;

import io.github.danduta.model.SensorRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class SensorProducerSimulator {

    private static final String DATASET_CSV_PATH = "DATASET_CSV_PATH";

    private static final Properties props = new Properties();
    private static final UUID producerId = UUID.randomUUID();
    private static final Logger log = Logger.getLogger(SensorProducerSimulator.class);

    static {
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            log.error("Error while loading properties from jar", e);
        }
    }

    public static void main(String[] args) {
        Admin kafkaAdmin = Admin.create(props);
        KafkaProducer<UUID, SensorRecord> producer = new KafkaProducer<>(props);

        try {
            String datasetPath = System.getenv(DATASET_CSV_PATH);
            assert datasetPath != null : "Could not resolve path to dataset CSV file";

            FileReader in = new FileReader(datasetPath);
            CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

            Map<String, Integer> headerMap = records.getHeaderMap();
            headerMap.remove("time");

            createKafkaTopics(kafkaAdmin, headerMap.keySet());

            for (CSVRecord record : records) {
                produceRecord(producer, record);
            }
        } catch (FileNotFoundException e) {
            log.error("Dataset could not be found", e);
        } catch (IOException e) {
            log.error("Exception occurred while reading the dataset", e);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static void produceRecord(KafkaProducer<UUID, SensorRecord> producer, CSVRecord record) {
        Map<String, String> recordHeaderMap = record.toMap();

        for (Entry<String, String> e : recordHeaderMap.entrySet()) {
            String topic = e.getKey();
            String valueString = e.getValue();

            Object kafkaValue;
            try {
                kafkaValue = Double.valueOf(valueString);
            } catch (NumberFormatException ignored) {
                kafkaValue = valueString;
            }

            ProducerRecord<UUID, SensorRecord> producerRecord = new ProducerRecord<>(topic,
                    producerId,
                    new SensorRecord(System.currentTimeMillis(), (Double) kafkaValue));

            producer.send(producerRecord);
        }
    }

    private static void createKafkaTopics(Admin kafkaAdmin, Set<String> newTopics) throws ExecutionException, InterruptedException {
        KafkaConsumer<Object, Object> dummyConsumer = new KafkaConsumer<>(props);

        Optional<Integer> partitions = Optional.empty();
        Optional<Short> replicationFactor = Optional.of((short) 1);

        for (String topic : newTopics) {
            try {
                List<NewTopic> topicWrapper = Collections.singletonList(new NewTopic(topic, partitions, replicationFactor));
                CreateTopicsResult topicsCreationResult = kafkaAdmin.createTopics(topicWrapper);
                topicsCreationResult.all().get();
            } catch (ExecutionException | InterruptedException e) {
                if (!(e.getCause() instanceof TopicExistsException))
                    throw e;

                log.info("Topic already existed: " + topic);
            }
        }
    }
}
