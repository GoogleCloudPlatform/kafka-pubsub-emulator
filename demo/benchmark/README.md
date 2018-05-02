# Pub/Sub Emulator for Kafka Benchmarker

This subfolder contains a simple tool to help benchmark the Pub/Sub Emulator for Kafka under various
Publisher and Subscriber. You can either run the tool against a running instance of the emulator 
connected to Kafka, or start your own Kafka cluster and emulator instance inside of docker.

## Starting Kafka and the Pub/Sub Emulator in Docker
From within this directory, issue `./docker-containers.sh start`. 

This script starts Zookeeper, three Kafka brokers, and the Pub/Sub Emulator exposed on port 8080
of your localhost machine. It also creates 4 topics, which are listed in the *producer.topics* 
section of the [config/application.yaml](./config/application.yaml) file in this directory.

If you want to change the topics and/or subscriptions, you will need to modify the script and the
configuration YAML file.
 
## Build and Running the Benchmarker
Build the benchmark program by running `mvn package`. Then, run the JAR file with the `--help` flag
to see the available parameters.

```bash
java -jar target/pubsub-emulator-benchmark-1.0-SNAPSHOT.jar --help
Usage: <main class> [options]
  Options:
  * -duration
       Duration (in seconds) to run the benchmark
       Default: 0
  * -emulator
       Hostname and port of the Pub/Sub Emulator server
  * -messageSize
       Size of message (in bytes)
       Default: 0
  * -publishers
       Number of Publisher clients to use
       Default: 0
  * -subscribers
       Number of Subscriber clients to use
       Default: 0
  * -subscription
       Name of Subscription to Pull from
  * -topic
       Name of Topic to Publish to
```

The following command would Publish and Subscribe from the 128-partition topic 
*performance-testing-128p* for 30s using 2 publishing threads and 2 StreamingPull subscribers and
1KB messages.

```bash
java -jar target/pubsub-emulator-benchmark-1.0-SNAPSHOT.jar \
    -emulator localhost:8080 \
    -topic performance-testing-128p \
    -subscription subscription-performance-testing-128p \
    -duration 30 \
    -publishers 2 \
    -subscribers 2 \
    -messageSize 1000
``` 

Status is printed every 10s and summarized at the end of the run.

```bash
2018-05-02T10:07:12.829EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark execute] INFO - Waiting for up to 30s for all Published messages to be received by Subscribers
2018-05-02T10:07:13.769EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark execute] INFO - Shutting down Subscribers
2018-05-02T10:07:13.886EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - After 31s
2018-05-02T10:07:13.886EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Published 344312 messages, 344.31 Mb (11.11 Mb/s) with 0 errors
2018-05-02T10:07:13.887EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Received 344312 messages, 367.55 Mb (11.86 Mb/s) with 23238 duplicates
2018-05-02T10:07:13.887EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Avg Publish Latency 350.06ms at 11106.84 qps
2018-05-02T10:07:13.888EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Server-captured Statistics
2018-05-02T10:07:13.888EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - --- Publisher Metrics ---
average_latency: 38.65, error_rate: 0.00, message_count: 344312, qps: 57.22, throughput: 57223.20
2018-05-02T10:07:13.888EDT [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - --- Subscriber Metrics ---
average_latency: 66.99, message_count: 367550, qps: 61.09, throughput: 61085.26
```