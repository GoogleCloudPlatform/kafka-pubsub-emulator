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

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.KAFKA_TOPIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.partners.pubsub.kafka.DefaultModule;
import com.google.cloud.partners.pubsub.kafka.KafkaClientFactory;
import com.google.cloud.partners.pubsub.kafka.PubsubEmulatorServer;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.cloud.partners.pubsub.kafka.integration.rule.KafkaRule;
import com.google.cloud.partners.pubsub.kafka.integration.rule.ZookeeperRule;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
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
  protected static final String CLIENT_LIBRARY_TOPIC = "publish-and-streaming-pull";
  protected static final String SSL_TOPIC = "ssl-server";
  protected static final String BROKER_FAILURE_TOPIC = "broker-failure";

  private static final Logger LOGGER = Logger.getLogger(BaseIT.class.getName());
  private static final String PROJECT = "emulator-testing";
  private static final String LOCALHOST = "localhost";
  private static final int PORT = 8080;
  private static final String CONFIG_FILE = "integration-test-config.json";
  private static final ExecutorService SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
  private static final CredentialsProvider NO_CREDENTIALS_PROVIDER = new NoCredentialsProvider();
  private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final ZookeeperRule ZOOKEEPER_RULE = new ZookeeperRule(TEMPORARY_FOLDER, 3);
  private static final ImmutableList<String> TOPICS =
      ImmutableList.of(BROKER_FAILURE_TOPIC, CLIENT_LIBRARY_TOPIC, SSL_TOPIC);

  protected static final KafkaRule KAFKA_RULE = new KafkaRule(TEMPORARY_FOLDER, ZOOKEEPER_RULE, 3);

  @ClassRule
  public static RuleChain CHAIN =
      RuleChain.outerRule(TEMPORARY_FOLDER).around(ZOOKEEPER_RULE).around(KAFKA_RULE);

  protected static boolean USE_SSL = false;
  private static PubsubEmulatorServer pubsubEmulatorServer;
  private static ConfigurationRepository configurationRepository;
  private static KafkaClientFactory kafkaClientFactory;

  /**
   * Configures Subscriptions and creates new Kafka topics for each test run to avoid interference
   * from previous runs.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Block until the PubsubEmulatorServer is running
    CompletableFuture<Void> serverStartedFuture = new CompletableFuture<>();
    LOGGER.info("Setting up integration test environment");
    TOPICS.forEach(
        topic -> {
          LOGGER.info("Trying to create " + topic);
          try {
            KAFKA_RULE.createTopic(topic);
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });

    Injector injector = getInjector();
    pubsubEmulatorServer = injector.getInstance(PubsubEmulatorServer.class);
    configurationRepository = injector.getInstance(ConfigurationRepository.class);
    kafkaClientFactory = injector.getInstance(KafkaClientFactory.class);

    SERVER_EXECUTOR.submit(
        () -> {
          try {
            pubsubEmulatorServer.start();
            serverStartedFuture.complete(null);
            pubsubEmulatorServer.blockUntilShutdown();
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
    pubsubEmulatorServer.stop();
    pubsubEmulatorServer.blockUntilShutdown();

    String[] topics =
        configurationRepository
            .getTopics(PROJECT)
            .stream()
            .map(topic -> topic.getLabelsOrDefault(KAFKA_TOPIC, topic.getName()))
            .toArray(String[]::new);
    KAFKA_RULE.deleteTopics(topics);
  }

  public static TransportChannelProvider getChannelProvider() {
    ManagedChannel channel = null;
    if (USE_SSL) {
      try {
        channel =
            NettyChannelBuilder.forAddress(LOCALHOST, PORT)
                .maxInboundMessageSize(100000)
                .sslContext(
                    GrpcSslContexts.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build())
                .overrideAuthority(LOCALHOST + ":" + PORT)
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
      File certificate =
          new File(configurationRepository.getServer().getSecurity().getCertificateChainFile());
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

  // Gets the Guice Injector based on the specified configuration
  private static Injector getInjector() throws IOException {
    if (!USE_SSL) {
      return Guice.createInjector(
          new DefaultModule(ClassLoader.getSystemResource(CONFIG_FILE).getPath()));
    } else {
      // Build a config, set the fully qualified path to the certs, and save
      File newConfig = TEMPORARY_FOLDER.newFile();
      String certPath = ClassLoader.getSystemResource("server.crt").getPath();
      String keyPath = ClassLoader.getSystemResource("server.key").getPath();
      String content =
          String.join(
              "\n",
              Files.readAllLines(
                  Paths.get(ClassLoader.getSystemResource(CONFIG_FILE).getPath()), UTF_8));
      Files.write(
          newConfig.toPath(),
          content
              .replace(
                  "\"port\": 8080",
                  "\"port\": 8080,\n"
                      + "    \"security\": {\n"
                      + "      \"certificateChainFile\": \""
                      + certPath
                      + "\",\n"
                      + "      \"privateKeyFile\": \""
                      + keyPath
                      + "\"\n"
                      + "    }")
              .getBytes(UTF_8));
      return Guice.createInjector(new DefaultModule(newConfig.getAbsolutePath()));
    }
  }

  protected Subscription getSubscriptionByTopic(String topicName) {
    return configurationRepository
        .getSubscriptionsForTopic(topicName)
        .stream()
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Topic name not found"));
  }

  /**
   * Creates a KafkaConsumer that is manually assigned to all partitions of the test topic indicated
   * by the {@code subscription}.
   */
  protected Consumer<String, ByteBuffer> getValidationConsumer(String topic, String subscription) {
    Consumer<String, ByteBuffer> consumer =
        kafkaClientFactory.createConsumer(
            ProjectSubscriptionName.of(PROJECT, subscription).toString());

    Set<TopicPartition> topicPartitions =
        consumer
            .listTopics()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().equals(ProjectTopicName.of(PROJECT, topic).toString()))
            .flatMap(
                e -> e.getValue().stream().map(p -> new TopicPartition(p.topic(), p.partition())))
            .collect(Collectors.toSet());
    consumer.assign(topicPartitions);

    return consumer;
  }

  protected Subscriber getSubscriber(String subscription, MessageReceiver receiver) {
    return Subscriber.newBuilder(ProjectSubscriptionName.of(PROJECT, subscription), receiver)
        .setChannelProvider(getChannelProvider())
        .setCredentialsProvider(NO_CREDENTIALS_PROVIDER)
        .build();
  }

  protected Publisher getPublisher(String topicName) throws IOException {
    return Publisher.newBuilder(ProjectTopicName.of(PROJECT, topicName))
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
