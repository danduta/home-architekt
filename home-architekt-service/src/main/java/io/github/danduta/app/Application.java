package io.github.danduta.app;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(Application.class.getName());
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        logger.info("Started Spark application.");
    }
}
