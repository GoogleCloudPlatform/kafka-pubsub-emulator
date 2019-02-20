# Pub/Sub Emulator for Kafka Benchmarker

This subfolder contains a simple tool to help benchmark the Pub/Sub Emulator for Kafka under various
Publisher and Subscriber. You can either run the tool against a running instance of the emulator 
connected to Kafka, or start your own Kafka cluster and emulator instance inside of docker.

## Starting Kafka and the Pub/Sub Emulator in Docker
From within this directory, issue `./docker-containers.sh start`. 

This script starts Zookeeper, three Kafka brokers, and the Pub/Sub Emulator exposed on port 8080
of your localhost machine. It also creates 4 topics, which can be found in the
[config/pubsub.json](./config/pubsub.json) file in this directory.

If you want to change the topics and/or subscriptions, you will need to modify the script and the
configuration JSON file.
 
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
    -messageSize
       Size of message (in bytes) (default: 1024)
       Default: 1024
    -publishQps
       Target QPS for Publishers (default: 100)
       Default: 100
    -publishers
       Number of Publisher clients to use (default: 2)
       Default: 2
    -subscribers
       Number of Subscriber clients to use (default: 2)
       Default: 2
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
    -topic projects/performance-testing/topics/performance-testing-128p \
    -subscription projects/performance-testing/subscriptions/subscription-performance-testing-128p \
    -duration 30 \
    -publishers 2 \
    -subscribers 2 \
    -messageSize 1000
``` 

Status is printed every 10s and summarized at the end of the run.

```bash
2019-02-20T14:24:09.334EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark execute] INFO - Waiting for up to 30s for all Published messages to be received by Subscribers
2019-02-20T14:24:09.336EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark execute] INFO - Shutting down Subscribers
2019-02-20T14:24:09.383EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - After 30s
2019-02-20T14:24:09.385EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Published 3011 messages, 3.01 Mb (0.10 Mb/s) with 0 errors
2019-02-20T14:24:09.386EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Received 3005 messages, 3.10 Mb (0.10 Mb/s) with 92 duplicates
2019-02-20T14:24:09.387EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Avg Publish Latency 159.98ms at 100.37 qps
2019-02-20T14:24:09.388EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - Server-captured Statistics
2019-02-20T14:24:09.389EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - --- Publisher Metrics ---
average_latency: 144.32, error_rate: 0.00, message_count: 3011, qps: 2.58, throughput: 2575.71
2019-02-20T14:24:09.390EST [com.google.cloud.partners.pubsub.emulator.PerformanceBenchmark summarizeResults] INFO - --- Subscriber Metrics ---
average_latency: 148.98, message_count: 3100, qps: 2.65, throughput: 2651.84
```