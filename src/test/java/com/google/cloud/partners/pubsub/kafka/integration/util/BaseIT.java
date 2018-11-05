/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.partners.pubsub.kafka.integration.util;

import static org.junit.Assert.fail;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.partners.pubsub.kafka.Configuration;
import com.google.cloud.partners.pubsub.kafka.KafkaClientFactoryImpl;
import com.google.cloud.partners.pubsub.kafka.PubsubEmulatorServer;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.integration.rule.KafkaRule;
import com.google.cloud.partners.pubsub.kafka.integration.rule.ZookeeperRule;
import com.google.cloud.partners.pubsub.kafka.properties.ConsumerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.SecurityProperties;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public abstract class BaseIT {

  protected static final String TOPIC_CONSUMER_OFFSETS = "__consumer_offsets";
  private static final Logger LOGGER = Logger.getLogger(BaseIT.class.getName());
  private static final String PROJECT = "cpe-ti";
  private static final String LOCALHOST = "localhost";
  private static final int PORT = 8080;
  private static final String CONFIGURATION_LOCALIZATION =
      "src/test/resources/application-integration-test.yaml";
  private static final ExecutorService SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
  private static final CredentialsProvider NO_CREDENTIALS_PROVIDER = new NoCredentialsProvider();
  private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final ZookeeperRule ZOOKEEPER_RULE = new ZookeeperRule(TEMPORARY_FOLDER, 3);
  protected static final KafkaRule KAFKA_RULE = new KafkaRule(TEMPORARY_FOLDER, ZOOKEEPER_RULE, 3);

  @ClassRule
  public static RuleChain CHAIN =
      RuleChain.outerRule(TEMPORARY_FOLDER).around(ZOOKEEPER_RULE).around(KAFKA_RULE);

  protected static boolean USE_SSL = false;
  private static PubsubEmulatorServer SERVER;

  /**
   * Configures Subscriptions and creates new Kafka topics for each test run to avoid interference
   * from previous runs.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Block until the PubsubEmulatorServer is running
    CompletableFuture<Void> serverStartedFuture = new CompletableFuture<>();
    SERVER = new PubsubEmulatorServer();
    Configuration.loadApplicationProperties(CONFIGURATION_LOCALIZATION);

    LOGGER.info("Setting up integration test environment");
    ConsumerProperties consumerProperties =
        Configuration.getApplicationProperties().getKafkaProperties().getConsumerProperties();

    for (SubscriptionProperties subscriptionProperty : consumerProperties.getSubscriptions()) {
      KAFKA_RULE.createTopic(subscriptionProperty.getTopic());
    }

    if (USE_SSL) {
      SecurityProperties securityProperties = new SecurityProperties();
      securityProperties.setCertChainFile("src/test/resources/static/server.crt");
      securityProperties.setPrivateKeyFile("src/test/resources/static/server.key");

      Configuration.getApplicationProperties()
          .getServerProperties()
          .setSecurityProperties(securityProperties);
    }

    SERVER_EXECUTOR.submit(
        () -> {
          try {
            SERVER.start();
            serverStartedFuture.complete(null);
            SERVER.blockUntilShutdown();
          } catch (IOException | InterruptedException e) {
            System.err.println("Unexpected server failure");
            serverStartedFuture.completeExceptionally(e);
          }
        });
    serverStartedFuture.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOGGER.info("Tearing down integration test environment");
    SERVER.stop();
    SERVER.blockUntilShutdown();

    Set<String> topics =
        Configuration.getApplicationProperties()
            .getKafkaProperties()
            .getConsumerProperties()
            .getSubscriptions()
            .stream()
            .map(SubscriptionProperties::getTopic)
            .collect(Collectors.toSet());

    topics.add(TOPIC_CONSUMER_OFFSETS);
    KAFKA_RULE.deleteTopics(topics.toArray(new String[topics.size()]));
  }

  public static TransportChannelProvider getChannelProvider() {
    ManagedChannel channel = null;
    if (USE_SSL) {
      SecurityProperties securityProperties =
          Configuration.getApplicationProperties().getServerProperties().getSecurityProperties();
      File certificate = new File(securityProperties.getCertChainFile());
      try {
        channel =
            NettyChannelBuilder.forAddress(LOCALHOST, PORT)
                .maxInboundMessageSize(100000)
                .sslContext(GrpcSslContexts.forClient().trustManager(certificate).build())
                .build();
      } catch (SSLException e) {
        fail("Unable to create SSL channel " + e.getMessage());
      }
    } else {
      channel = ManagedChannelBuilder.forAddress(LOCALHOST, PORT).usePlaintext(true).build();
    }
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
  }

  public static AdminGrpc.AdminBlockingStub getAdminStub() {
    ManagedChannel channel = null;
    if (USE_SSL) {
      SecurityProperties securityProperties =
          Configuration.getApplicationProperties().getServerProperties().getSecurityProperties();
      File certificate = new File(securityProperties.getCertChainFile());
      try {
        channel =
            NettyChannelBuilder.forAddress(LOCALHOST, PORT)
                .maxInboundMessageSize(100000)
                .sslContext(GrpcSslContexts.forClient().trustManager(certificate).build())
                .build();
      } catch (SSLException e) {
        fail("Unable to create SSL channel " + e.getMessage());
      }
    } else {
      channel = ManagedChannelBuilder.forAddress(LOCALHOST, PORT).usePlaintext(true).build();
    }
    return AdminGrpc.newBlockingStub(channel);
  }

  protected SubscriptionProperties getSubscriptionPropertiesByTopic(String topicName) {
    return Configuration.getApplicationProperties()
        .getKafkaProperties()
        .getConsumerProperties()
        .getSubscriptions()
        .stream()
        .filter(s -> s.getTopic().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Topic name not found"));
  }

  /**
   * Creates a KafkaConsumer that is manually assigned to all partitions of the test topic indicated
   * by the {@code subscription}.
   */
  protected Consumer<String, ByteBuffer> getValidationConsumer(
      SubscriptionProperties subscriptionProperties) {
    Consumer<String, ByteBuffer> consumer =
        new KafkaClientFactoryImpl().createConsumer(subscriptionProperties.getName());

    Set<TopicPartition> topicPartitions =
        consumer
            .listTopics()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().equals(subscriptionProperties.getTopic()))
            .flatMap(
                e -> e.getValue().stream().map(p -> new TopicPartition(p.topic(), p.partition())))
            .collect(Collectors.toSet());
    consumer.assign(topicPartitions);

    return consumer;
  }

  protected Subscriber getSubscriber(
      SubscriptionProperties subscriptionProperties, MessageReceiver receiver) {
    return Subscriber.newBuilder(
            ProjectSubscriptionName.of(PROJECT, subscriptionProperties.getName()), receiver)
        .setChannelProvider(getChannelProvider())
        .setCredentialsProvider(NO_CREDENTIALS_PROVIDER)
        .build();
  }

  protected Publisher getPublisher(SubscriptionProperties subscriptionProperties)
      throws IOException {
    return Publisher.newBuilder(ProjectTopicName.of(PROJECT, subscriptionProperties.getTopic()))
        .setChannelProvider(getChannelProvider())
        .setCredentialsProvider(NO_CREDENTIALS_PROVIDER)
        .build();
  }

  protected void publish(
      Publisher publisher,
      PubsubMessage message,
      java.util.function.Consumer<Throwable> onFailure,
      java.util.function.Consumer<String> onSuccess) {
    ApiFutures.addCallback(
        publisher.publish(message),
        new ApiFutureCallback<String>() {
          @Override
          public void onFailure(Throwable throwable) {
            onFailure.accept(throwable);
          }

          @Override
          public void onSuccess(String result) {
            onSuccess.accept(result);
          }
        });
  }
}
