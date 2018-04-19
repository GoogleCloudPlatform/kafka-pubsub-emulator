# Pub/Sub Emulator for Kafka

This project implements a gRPC server that satisfies the [Cloud Pub/Sub 
API](https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#index) as an emulation 
layer on top of an existing Kafka cluster configuration. The emulator is exposed as a standalone
Java application with a mandatory configuration file passed as an argument at runtime. It is fully
compatible with the latest versions of the Google Cloud Client libraries and is explicitly tested
against the [Java version](https://googlecloudplatform.github.io/google-cloud-java/).

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/kafka-pubsub-emulator.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/kafka-pubsub-emulator)
[![Coverage](https://codecov.io/gh/GoogleCloudPlatform/kafka-pubsub-emulator/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/kafka-pubsub-emulator)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/de15f86f62974e87bf6cfc9edc6fb51b)](https://www.codacy.com/app/prodonjs/kafka-pubsub-emulator?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=GoogleCloudPlatform/kafka-pubsub-emulator&amp;utm_campaign=Badge_Grade)

## Building and Running
Checkout the source and then build the self-contained JAR with `mvn package`. If you wish to run the
integration test suite, which starts an in-memory Kafka cluster, run `mvn verify`. 
The self-contained JAR is then available at `./target/kafka-pubsub-emulator-<version>.jar`.

### Standalone JAR
When invoking the standalone JAR, you must specify the location of a configuration file using
the `--configuration.location` command line argument.

This is an example using the [default configuration](./src/main/resources/application.yaml)
from the resources folder. This configuration assumes that you have Kafka running locally on ports
9094, 9095, and 9096 and topics named *throughput-testing-3p*, *throughput-testing-6p*, 
*throughput-testing-100p*, and *testing-10p* respectively.
```
java -jar target/kafka-pubsub-emulator-1.0.0.0.jar \
  --configuration.location=src/main/resources/application.yaml
```

Most likely, you will need to specify your own configuration file as described in the
 [Configuration Options](#configuration-options) section below.

### Docker

To execute docker container, you must provide a volume with configuration file.

This is an example using the default configuration from the resources folder. This configuration assumes that you have Kafka running locally on ports 9094, 9095, and 9096 and topics named throughput-testing-3p, throughput-testing-6p, throughput-testing-100p, and testing-10p respectively.

```
export PATH_TO_CONFIG="/insert/path/to/configuration/file.yaml"

docker build -t kafka-pubsub-emulator:1.0.0.0 .

docker run --mount type=bind,src=${PATH_TO_CONFIG},dst=/etc/config -d kafka-pub-sub-emulator:1.0.0.0 \
  --configuration.location=/etc/config/application.yaml
```

### Kubernetes

The configuration for kubernetes was based on Minikube, to configure see more [here](https://kubernetes.io/docs/tutorials/stateless-application/hello-minikube/).

This is an example using the default configuration from the resources folder. This configuration assumes that you have Kafka running locally on ports 9094, 9095, and 9096 and topics named throughput-testing-3p, throughput-testing-6p, throughput-testing-100p, and testing-10p respectively.

Build application container:
```
docker build -t kafka-pubsub-emulator:1.0.0.0 .
```
Expose application configuration on [Config Map](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/).
```
kubectl create configmap application-config --from-file=src/main/resources/application.yaml 
```
Create deployment [see more](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), with 1 application pods. 
```
kubectl create -f kubernetes/kafka-pubsub-emulator-deployment.yaml
```
Create service load balancer for emulator application [see more](https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/). 
```
kubectl expose deployment kafka-pubsub-emulator --type=LoadBalancer
```

### Configuration Options
The Pub/Sub Emulator server needs to be started with a YAML-based configuration file that indicates
that specifies the addresses of the Kafka brokers, as well as the Topics and Subscriptions that
the emulator will support. See 
[src/main/resources/application.yaml](./src/main/resources/application.yaml) for the proper format.

#### Required Options
- **server.port**: This is the port number the emulator will make available for clients. 
- **kafka.bootstrapServers**: Comma-separated list of broker addresses (host:port)
- **kafka.consumer.subscriptions**: This is a list containing objects representing 
[Subscriptions](https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.Subscription)
as defined by the Pub/Sub API. The name, the topic it's bound to, and the acknowledgement deadline
are required. Create and Delete operations, as well as push configurations are not supported at this
time.
- **kafka.producer.topics**: This is a list of existing Kafka topics that the emulator exposes for
publishing. *Note that if your broker configuration supports auto-creation of topics, it's possible
to specify topics that do not exist within the cluster*.

#### Optional Options
- **server.parallelism**: Specifies the maximum number of threads to use to satisfy gRPC requests. 
By default, this is set to the number of CPU cores.
- **server.security.certChainFile**: Path to the certificate chain file.
- **server.security.privateKeyFile**: Path to the private key file. When combined with the 
certChainFile option, these enable the server to use SSL/TLS encryption for all communication rather
than the default plaintext channel.
- **kafka.consumer.executors**: Specifies the number of KafkaConsumers to use per Subscription. 
By default, this is set to the number of CPU cores. Regardless of the setting, the emulator will
only create as many consumers as the number of partitions in the topic. Increasing this setting may
increase throughput but will incur a greater number of TCP connections to the brokers.
- **kafka.consumer.properties**: This section provides a means for tweaking the behavior of each
KafkaConsumer client used in the Subscriber implementation by setting values for any of the 
[KafkaConsumer configs](https://kafka.apache.org/documentation/#consumerconfigs). *Note that certain
options are always overriden by the server in order to ensure proper emulator behavior. 
See [KafkaClientFactoryImpl](./src/main/java/com/google/cloud/partners/pubsub/kafka/KafkaClientFactoryImpl.java)
for more details.*
- **kafka.producer.executors**: Specifies the number of KafkaProducers to use for publishing. 
By default, this is set to the number of CPU cores. Increasing this setting may
increase throughput but will incur a greater number of TCP connections to the brokers.
- **kafka.producer.properties**: This section provides a means for tweaking the behavior of each
KafkaProducer client used in the Publisher implementation by setting values for any of the 
[KafkaProducer configs](https://kafka.apache.org/documentation/#producerconfigs). *Note that certain
options are always overriden by the server in order to ensure proper emulator behavior. 
See [KafkaClientFactoryImpl](./src/main/java/com/google/cloud/partners/pubsub/kafka/KafkaClientFactoryImpl.java)
for more details.*

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

For further reference, consult the examples in the [integration tests](./src/test/java/integration).
