package io.github.danduta.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import java.util.stream.Collectors;

public class SensorProducerSimulator {

    private static final String DATASET_CSV_PATH = "DATASET_CSV_PATH";
    private static final String THREAD_COUNT = "PRODUCER_THREAD_COUNT";
    private static final String PRODUCER_TOPICS = "PRODUCER_TOPICS";
    private static final String KAFKA_ENDPOINT = "KAFKA_ENDPOINT";
    private static final String PARTITION_COUNT = "KAFKA_TOPIC_PARTITION_COUNT";
    private static final String REPLICATION_FACTOR = "KAFKA_TOPIC_REPLICATION_FACTOR";

    private static final Set<String> REQUIRED_ENV_VARS = Set.of(
            DATASET_CSV_PATH,
            THREAD_COUNT,
            PRODUCER_TOPICS,
            KAFKA_ENDPOINT,
            PARTITION_COUNT,
            REPLICATION_FACTOR
    );

    private static final String DEFAULT_TOPIC = "use";
    private static final int DEFAULT_THREAD_COUNT = 5;

    private static int partitionCount;
    private static short replicationFactor;

    private static final Properties props = new Properties();
    private static final Logger log = Logger.getLogger(SensorProducerSimulator.class);

    private static ExecutorService executorService;
    private static List<NoisyRecordProducer> generators;

    static {
        try {
            checkEnvVars();

            partitionCount = Integer.parseInt(System.getenv(PARTITION_COUNT));
            replicationFactor = Short.parseShort(System.getenv(REPLICATION_FACTOR));

            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties"));
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KAFKA_ENDPOINT));

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

    private static void checkEnvVars() {
        Set<String> missingEnvVars = new HashSet<>(REQUIRED_ENV_VARS);
        missingEnvVars.removeIf(var -> System.getenv(var) != null);

        if (!missingEnvVars.isEmpty()) {
            throw new RuntimeException("The following environment variables are missing: " + missingEnvVars);
        }

    }

    public static void main(String[] args) {
        Admin kafkaAdmin = Admin.create(props);

        String topicsString = System.getenv(PRODUCER_TOPICS);
        List<String> topics = topicsString != null ?
                Arrays.asList(topicsString.split("\\s*,\\s*")) :
                Collections.singletonList(DEFAULT_TOPIC);

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

            log.info("Successfully produced a total of " + records.getRecordNumber() * generators.size() + " records.");
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

            generators.forEach(generator -> {
                generator.setCurrent(kafkaValue, kafkaTopic);
                executorService.submit(generator);
            });

        }
    }

    private static void createKafkaTopics(Admin kafkaAdmin, List<String> newTopics) throws ExecutionException, InterruptedException {
        Optional<Integer> topicPartitions = Optional.of(partitionCount);
        Optional<Short> topicReplicationFactor = Optional.of(replicationFactor);

        try {
            List<NewTopic> kafkaTopics = newTopics.stream().map(topicName -> new NewTopic(topicName, topicPartitions, topicReplicationFactor)).collect(Collectors.toList());
            CreateTopicsResult topicsCreationResult = kafkaAdmin.createTopics(kafkaTopics);
            topicsCreationResult.all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (!(e.getCause() instanceof TopicExistsException))
                throw e;
        }
    }
}
