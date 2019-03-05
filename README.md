# Pub/Sub Emulator for Kafka

This project implements a gRPC server that satisfies the [Cloud Pub/Sub 
API](https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#index) as an emulation 
layer on top of an existing Kafka cluster configuration. The emulator is exposed as a standalone
Java application with a mandatory configuration passed as an argument at runtime. It is fully
compatible with the latest versions of the Google Cloud Client libraries and is explicitly tested
against the [Java client library](https://googlecloudplatform.github.io/google-cloud-java/).

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/kafka-pubsub-emulator.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/kafka-pubsub-emulator)
[![Coverage](https://codecov.io/gh/GoogleCloudPlatform/kafka-pubsub-emulator/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/kafka-pubsub-emulator)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/de15f86f62974e87bf6cfc9edc6fb51b)](https://www.codacy.com/app/prodonjs/kafka-pubsub-emulator?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=GoogleCloudPlatform/kafka-pubsub-emulator&amp;utm_campaign=Badge_Grade)

## Building and Running

Checkout the source and then build the self-contained JAR with `mvn package`. If you wish to run the
integration test suite, which starts an in-memory Kafka cluster, run `mvn verify`. 
The self-contained JAR is then available at `./target/kafka-pubsub-emulator-<version>.jar`.

### Standalone JAR

When invoking the standalone JAR, you must specify the location of a JSON-formatted configuration 
file using the `-c` command line argument.

A sample configuration can be found at [./demo/benchmark/config/config.json](./demo/benchmark/config/config.json).
This configuration assumes you have Kafka accessible via three hosts, kafka-0, kafka-1, and kafka-2
each at port 9092.

You must also provide a JSON-formatted file containing Pub/Sub project definitions using the `-p`
command line argument. These entries define the projects available to the emulator's clients, along
with the Topics and Subscriptions that will be present when the server is started. This file must
be writeable if your clients will be creating, deleting, or modifying Topics and Subscriptions.

A sample configuration can be found at [./demo/benchmark/config/pubsub.json](./demo/benchmark/config/pubsub.json).
This provides for a single project named **performance-testing**, 4 Topics, and one Subscription per
topic.

```
java -jar target/kafka-pubsub-emulator-<POM version>.jar \
  -c <path to your server configuration file> \
  -p <path to your Pub/Sub file>
```

Most likely, you will need to specify your own configuration file as described in the
 [Configuration Options](#configuration-options) section below.

### Docker

To execute the emulator using Docker, you must provide it with the necessary configurations. For an
example, see the [./demo/benchmark/docker-containers.sh](./demo/benchmark/docker-containers.sh)
script.

```
export PATH_TO_CONFIGS="/path/to/configuration-files/"

docker build -t kafka-pubsub-emulator:<POM version> .

docker run --mount type=bind,src=${PATH_TO_CONFIGS},dst=/etc/config \
  -d kafka-pub-sub-emulator:<POM version> \
  -c /etc/config/config.json -p /etc/config/pubsub.json
```

### Kubernetes

Updates coming soon...

### Configuration Options

The Pub/Sub Emulator server needs to be started with a JSON-based configuration file that indicates
the server port, optional TLS settings, and the information needed to connect to a Kafka cluster. 
See [src/main/proto/config.proto](./src/main/proto/config.proto) for the full structure.

#### Required Options

- **port**: This is the port number the emulator will make available for clients. 
- **kafka.bootstrap_servers**: List of broker addresses (host:port)
- **kafka.producer_executors**: Number of executor threads used when publishing messages to Kafka.
  Increasing this setting may increase throughput but will incur a greater number of TCP connections
  to the brokers.
- **kafka.consumers_per_subscription**: Number of executor threads used to pull messages from a 
  Kafka topic within a Subscription. Increasing this number will allow for better Subscriber 
  throughput. Regardless of the setting, the emulator will only create at most one consumer per
  Kafka topic partition.    
    
#### Optional Options

- **security.certificate_chain_file**: Path to the certificate chain file.
- **security.private_key_file**: Path to the private key file. When combined with the 
  certificate_chain_file option, these enable the server to use SSL/TLS encryption for all 
  communication rather than the default plaintext channel.
- **kafka.consumer_properties**: This section provides a means for tweaking the behavior of each
  KafkaConsumer client used in the Subscriber implementation by setting values for any of the 
  [KafkaConsumer configs](https://kafka.apache.org/documentation/#consumerconfigs). *Note that certain
  options are always overriden by the server in order to ensure proper emulator behavior. 
  See [KafkaClientFactoryImpl](./src/main/java/com/google/cloud/partners/pubsub/kafka/KafkaClientFactoryImpl.java)
  for more details.*
- **kafka.producer_properties**: This section provides a means for tweaking the behavior of each
  KafkaProducer client used in the Publisher implementation by setting values for any of the 
  [KafkaProducer configs](https://kafka.apache.org/documentation/#producerconfigs). *Note that
  certain options are always overriden by the server in order to ensure proper emulator behavior. 
  See [KafkaClientFactoryImpl](./src/main/java/com/google/cloud/partners/pubsub/kafka/KafkaClientFactoryImpl.java)
  for more details.*
  
### Pub/Sub Configuration Repository

The Pub/Sub configuration must be bootstrapped when the emulator is started. Currently, there is a
single implementation of the [PubSubRepository](./src/main/java/com/google/cloud/partners/pubsub/kafka/config/PubSubRepository.java)
interface, which supports reading from and writing back to a file. This file must be provided
with the `-p` command-line argument and should be a JSON-formatted Protocol buffer message matching
the [PubSub](./src/main/proto/config.proto) message type.

A valid configuration must have at least one Project. Each Project can have many Topics, each of
which may have one or more Subscriptions to it. Within a Topic, the `kafka_topic` property can be
used in situations where the Kafka topic being bound to has a different name than what will be
exposed through the emulator.  

## Connecting with Google Cloud Client Library for Java

To connect client applications, you can use the official Google Cloud Platform client libraries
for your preferred language. The examples below assume you are using the 
[Java](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-pubsub) 
libraries. These settings can be adapted for other languages.

### Setting explicit CredentialsProvider and ChannelProvider

By default, the client library attempts to use your Google Cloud Project credentials as explained
in the [Authentication](https://github.com/GoogleCloudPlatform/google-cloud-java#authentication)
docs, and connects to pubsub.googleapis.com. Since the emulator does not run in GCP,
you'll need to specify a custom CredentialsProvider and create a new Channel bound to the emulator's
host and port.

Below is an example that will create a `Publisher` client connected to the emulator server running
on the local machine at port 8080 using a plaintext connection.

```java
ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
        .usePlaintext(true)
        .build();
Publisher publisher = Publisher.newBuilder(ProjectTopicName.of("my-project", "my-topic")
        .setChannelProvider(
            FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
        .setCredentialsProvider(new NoCredentialsProvider())
        .build();
```

If your emulator server is using SSL/TLS, you will need to create a secure Channel using a slightly
different instantiation pattern.

```java
File certificate = new File("path-to-certificate.crt");
ManagedChannel channel;
try {
  channel =
      NettyChannelBuilder.forAddress("localhost", 8080)
          .maxInboundMessageSize(100000)
          .sslContext(GrpcSslContexts.forClient().trustManager(certificate).build())
          .build();
} catch (SSLException e) {
  System.err.println("Unable to create SSL channel " + e.getMessage());
}
Publisher publisher = Publisher.newBuilder(ProjectTopicName.of("my-project", "my-topic")
        .setChannelProvider(
            FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
        .setCredentialsProvider(new NoCredentialsProvider())
        .build();
```

One difference between how the client library behaves with the emulator vs. Cloud Pub/Sub is that
by default, clients connected to Cloud Pub/Sub open multiple channels (1 per CPU core). Currently,
it's not possible to specify that type of multi-channel configuration with the emulator without
writing custom Channel code.

For further reference, consult the examples in the
 [integration tests](./src/test/java/com/google/cloud/partners/pubsub/kafka/integration) or look
 at the [demos](./demo) for various platforms.
