package io.github.danduta.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SensorProducerSimulator {

    private static final String DATASET_CSV_PATH = "DATASET_CSV_PATH";
    private static final String THREAD_COUNT = "PRODUCER_THREAD_COUNT";
    private static final String PRODUCER_TOPICS = "PRODUCER_TOPICS";

    private static final String DEFAULT_TOPIC = "use";
    private static final int DEFAULT_THREAD_COUNT = 5;

    private static final Properties props = new Properties();
    private static final Logger log = Logger.getLogger(SensorProducerSimulator.class);

    private static ExecutorService executorService;
    private static List<NoisyRecordProducer> generators;

    static {
        try {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties"));

            int threadCount = System.getenv(THREAD_COUNT) != null ?
                    Integer.parseInt(System.getenv(THREAD_COUNT)) : DEFAULT_THREAD_COUNT;

            executorService = Executors.newFixedThreadPool(2 * threadCount);
            generators = new ArrayList<>(threadCount);

            for (int i = 0; i < threadCount; i++) {
                generators.add(new NoisyRecordProducer(props));
            }
        } catch (IOException e) {
            log.error("Error while loading properties from jar", e);
        } catch (NumberFormatException e) {
            log.error("Couldn't parse thread count environment variable", e);
        }
    }

    public static void main(String[] args) {
        Admin kafkaAdmin = Admin.create(props);

        String topicsString = System.getenv(PRODUCER_TOPICS);
        Set<String> topics = topicsString != null ?
                new HashSet<>(Arrays.asList(topicsString.split("\\s*,\\s*"))) :
                new HashSet<>(Collections.singletonList(DEFAULT_TOPIC));

        try {
            String datasetPath = System.getenv(DATASET_CSV_PATH);
            assert datasetPath != null : "Could not resolve path to dataset CSV file";

            FileReader in = new FileReader(datasetPath);
            CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

            Map<String, Integer> headerMap = records.getHeaderMap();
            assert headerMap.keySet().containsAll(topics);

            createKafkaTopics(kafkaAdmin, topics);

            for (CSVRecord record : records) {
                Map<String, String> trimmedRecord = record.toMap();
                trimmedRecord.entrySet().removeIf(entry -> !topics.contains(entry.getKey()));

                processRecordRow(trimmedRecord, executorService);

                long idx = record.getRecordNumber();
                if (idx % 1000 == 0) {
                    log.info(String.format("Successfully processed %d CSV records for a total of %d...", idx, idx * generators.size()));
                }
            }

            log.info("Successfully produced a total of " + records.getRecordNumber() + " records.");
        } catch (FileNotFoundException e) {
            log.error("Dataset could not be found", e);
        } catch (IOException e) {
            log.error("Exception occurred while reading the dataset", e);
        } catch (Exception e) {
            log.error(e);
        } finally {
            generators.forEach(NoisyRecordProducer::flush);
            generators.forEach(NoisyRecordProducer::close);

            executorService.shutdown();
        }
    }

    private static void processRecordRow(Map<String, String> record, ExecutorService executorService) throws InterruptedException {

        for (Entry<String, String> e : record.entrySet()) {
            String kafkaTopic = e.getKey();
            String valueString = e.getValue();

            double kafkaValue;
            try {
                kafkaValue = Double.parseDouble(valueString);
            } catch (NumberFormatException ignored) {
                continue;
            }

            generators.forEach(generator -> generator.setCurrent(kafkaValue, kafkaTopic));
            executorService.invokeAll(generators);
        }
    }

    private static void createKafkaTopics(Admin kafkaAdmin, Set<String> newTopics) throws ExecutionException, InterruptedException {
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

                log.warn("Topic already existed: " + topic);
            }
        }
    }
}
