package io.github.danduta.producer;

import kafka.admin.AdminUtils;
import kafka.admin.AdminUtils$;
import kafka.zk.KafkaZkClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
        KafkaProducer<UUID, Double> numericalDataProducer = new KafkaProducer<>(props);

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<UUID, String> stringDataProducer = new KafkaProducer<>(props);

        try {
            String datasetPath = System.getenv(DATASET_CSV_PATH);
            assert datasetPath != null : "Could not resolve path to dataset CSV file";

            FileReader in = new FileReader(datasetPath);
            CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

            Map<String, Integer> headerMap = records.getHeaderMap();
            headerMap.remove("time");

            createKafkaTopics(kafkaAdmin, headerMap.keySet());

            for (CSVRecord record : records) {
                produceRecord(numericalDataProducer, stringDataProducer, record);
            }
        } catch (FileNotFoundException e) {
            log.error("Dataset could not be found", e);
        } catch (IOException e) {
            log.error("Exception occurred while reading the dataset", e);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            numericalDataProducer.flush();
            numericalDataProducer.close();

            stringDataProducer.flush();
            stringDataProducer.close();
        }
    }

    private static void produceRecord(KafkaProducer<UUID, Double> numericalDataProducer, KafkaProducer<UUID, String> stringDataProducer, CSVRecord record) {
        Map<String, String> recordHeaderMap = record.toMap();

        for (Entry<String, String> e : recordHeaderMap.entrySet()) {
            String topic = e.getKey();
            String valueString = e.getValue();

            try {
                ProducerRecord<UUID, Double> producerRecord = new ProducerRecord<>(topic,
                        producerId,
                        Double.valueOf(valueString));

                numericalDataProducer.send(producerRecord);
            } catch (NumberFormatException ex) {
                ProducerRecord<UUID, String> producerRecord = new ProducerRecord<>(topic,
                        producerId,
                        valueString);

                stringDataProducer.send(producerRecord);
            }
        }
    }

    private static void createKafkaTopics(Admin kafkaAdmin, Set<String> newTopics) throws InterruptedException, ExecutionException {
        KafkaConsumer<Object, Object> dummyConsumer = new KafkaConsumer<>(props);
        Set<String> existentTopics = dummyConsumer.listTopics(Duration.of(60, ChronoUnit.SECONDS)).keySet();

        log.info("Existent topics: " + existentTopics);

        newTopics.removeAll(existentTopics);

        if (newTopics.isEmpty()) {
            log.info("All needed topics are already created");
            return;
        }

        log.info("Creating new topics: " + newTopics);

        Optional<Integer> partitions = Optional.empty();
        Optional<Short> replicationFactor = Optional.of((short) 1);
        List<NewTopic> kafkaTopics = newTopics.stream().map(column -> new NewTopic(column, partitions, replicationFactor)).collect(Collectors.toList());

        CreateTopicsResult topicsCreationResult = kafkaAdmin.createTopics(kafkaTopics);
        topicsCreationResult.all().get();
    }
}
