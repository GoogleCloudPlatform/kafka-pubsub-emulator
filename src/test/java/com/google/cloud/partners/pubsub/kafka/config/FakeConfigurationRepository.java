package com.google.cloud.partners.pubsub.kafka.config;

import com.google.cloud.partners.pubsub.kafka.config.Server.Security;
import com.google.common.collect.ImmutableList;

public class FakeConfigurationRepository extends ConfigurationRepository {

  private static final Configuration CONFIGURATION =
      Configuration.newBuilder()
          .setServer(
              Server.newBuilder()
                  .setPort(8080)
                  .setSecurity(
                      Security.newBuilder()
                          .setCertificateChainFile("/path/to/server.crt")
                          .setPrivateKeyFile("/path/to/server.key")
                          .build())
                  .build())
          .setKafka(
              Kafka.newBuilder()
                  .addAllBootstrapServers(ImmutableList.of("server1:2192", "server2:2192"))
                  .putProducerProperties("max.poll.records", "1000")
                  .putConsumerProperties("linger.ms", "5")
                  .putConsumerProperties("batch.size", "1000000")
                  .putConsumerProperties("buffer.memory", "32000000")
                  .setProducerExecutors(4)
                  .setConsumersPerSubscription(4)
                  .build())
          .setPubsub(
              PubSub.newBuilder()
                  .addProjects(
                      Project.newBuilder()
                          .setName("project-1")
                          .addTopics(
                              Topic.newBuilder()
                                  .setName("topic-1")
                                  .setKafkaTopic("kafka-topic-1")
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-1")
                                          .setAckDeadlineSeconds(10)
                                          .build())
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-2")
                                          .setAckDeadlineSeconds(10)
                                          .build())
                                  .build())
                          .addTopics(
                              Topic.newBuilder()
                                  .setName("topic-2")
                                  .setKafkaTopic("kafka-topic-2")
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-3")
                                          .setAckDeadlineSeconds(30)
                                          .build())
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-4")
                                          .setAckDeadlineSeconds(45)
                                          .build())
                                  .build())
                          .build())
                  .addProjects(
                      Project.newBuilder()
                          .setName("project-2")
                          .addTopics(
                              Topic.newBuilder()
                                  .setName("topic-1")
                                  .setKafkaTopic("kafka-topic-1")
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-1")
                                          .setAckDeadlineSeconds(10)
                                          .build())
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-2")
                                          .setAckDeadlineSeconds(10)
                                          .build())
                                  .build())
                          .addTopics(
                              Topic.newBuilder()
                                  .setName("topic-2")
                                  .setKafkaTopic("kafka-topic-2")
                                  .addSubscriptions(
                                      Subscription.newBuilder()
                                          .setName("subscription-3")
                                          .setAckDeadlineSeconds(30)
                                          .build())
                                  .build())
                          .build())
                  .build())
          .build();

  public FakeConfigurationRepository() {
    super(CONFIGURATION);
  }

  public FakeConfigurationRepository(Configuration configuration) {
    super(configuration);
  }

  @Override
  public void save() {}
}
