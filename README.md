## A large scale streaming and data processing architecture ##

This repo contains all the code and infrastructure necessary for deploying a PoC data streaming and processing platform
based on Kafka and Spark Streaming. This project represents my bachelor thesis, sustained at the Faculty of Automatic
Control and Computers in July 2022.

### Abstract ###

This project focuses on the design and development of a data processing architecture for high density data streams. Many
methods of data processing can use advanced Machine Learning algorithms or statistical methods, but cannot handle large
amounts of data, or data which arrives at a very high frequency. The resulting architecture will focus on extracting
knowledge from high frequency data, such as outlier detection or classification methods. As a result, the Data Analysis
Engine could send notifications if parts of the data are missing, incorrect or exceed certain thresholds. The
architecture will be based on Spark and Spark Streaming for data processing, with connectors developed to receive
different streams of data.

### How to run ###

#### Installing the hem chart ####
Firstly, you need to make sure that the Helm chart's dependencies are met. The chart is not pushed to any repository, so
this step is mandatory.

```shell
$ helm dependency build
```

Now you can install the Helm chart in the Kubernetes cluster:

```shell
$ helm install release-name infra/outlier-detector
```
<br>

#### Deploying only the Spark application in a Kubernetes cluster
Alternatively, you can deploy the Spark application directly with the `outlier-detector.yaml` file provided in the `infra` directory.
First, you need to make sure that the environment provided in `outlier-detector.yaml` points to the right Kafka and Spark endpoints.
Then run the following command in a shell where `kubectl` is configured:

```shell
$ kubectl apply -f infra/outline-detector.yaml
```

<br>
The producer simulation can be built by packaging it with Maven.

```shell
$ mvn package
```

To run the producer simulation, a few environment variables need to be set as follows:

| Env var name                     | Description                                                                                        | Example value          |
|----------------------------------|----------------------------------------------------------------------------------------------------|------------------------|
| `DATASET_CSV_PATH`               | The path to the dataset.csv file that the produced data is based on                                | `/path/to/dataset.csv` |
| `PRODUCER_THREAD_COUNT`          | Number of producers to run in parallel                                                             | 10                     |
| `KAFKA_ENDPOINT`                 | External IP of the Kafka LoadBalancer                                                              | http://1.2.3.4:9092    |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | Replication factor for the Kafka topics created by the producer                                    | 2                      |
| `KAFKA_TOPIC_PARTITION_COUNT`    | Partition count for the Kafka topics created by the producer                                       | 2                      |
| `PRODUCER_TOPICS`                | Columns read from the dataset/Kafka topics created. By default, the producer sends all the columns | use                    |
| `LOOP_PRODUCER`                  | Continuosly read the dataset from the beginning in a loop                                          | false                  |

After providing the right environment, the simulation can be run with the following command:
```shell
$ java -jar home-architekt-producer/target/home-architekt-producer-1.0-jar-with-dependencies.jar
```
