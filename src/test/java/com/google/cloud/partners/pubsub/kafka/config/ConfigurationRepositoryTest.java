package com.google.cloud.partners.pubsub.kafka.config;

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.KAFKA_TOPIC;
import static org.junit.Assert.assertThat;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationAlreadyExistsException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationNotFoundException;
import com.google.cloud.partners.pubsub.kafka.config.Server.Security;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConfigurationRepositoryTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();
  private ConfigurationRepository configurationRepository;

  @Before
  public void setUp() {
    configurationRepository = new TestConfigurationRepository();
  }

  @Test
  public void getTopics() {
    assertThat(
        configurationRepository.getTopics("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .build(),
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
    assertThat(
        configurationRepository.getTopics("projects/project-2"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .build(),
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-2/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
  }

  @Test
  public void getTopics_projectNotFound() {
    assertThat(configurationRepository.getTopics("projects/missing"), Matchers.empty());
  }

  @Test
  public void getSubscriptions() {
    assertThat(
        configurationRepository.getSubscriptions("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-3")
                .setTopic("projects/project-1/topics/topic-2")
                .setAckDeadlineSeconds(30)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-4")
                .setTopic("projects/project-1/topics/topic-2")
                .setAckDeadlineSeconds(45)
                .build()));
    assertThat(
        configurationRepository.getSubscriptions("projects/project-2"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-1")
                .setTopic("projects/project-2/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-2")
                .setTopic("projects/project-2/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-3")
                .setTopic("projects/project-2/topics/topic-2")
                .setAckDeadlineSeconds(30)
                .build()));
  }

  @Test
  public void getSubscriptions_projectNotFound() {
    assertThat(configurationRepository.getSubscriptions("projects/missing"), Matchers.empty());
  }

  @Test
  public void createTopic() throws ConfigurationException {
    com.google.pubsub.v1.Topic newTopic =
        com.google.pubsub.v1.Topic.newBuilder()
            .setName("projects/project-1/topics/a-new-topic")
            .build();
    configurationRepository.createTopic(newTopic);
    assertThat(
        configurationRepository.getTopics("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/a-new-topic")
                .putLabels(KAFKA_TOPIC, "a-new-topic")
                .build(),
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .build(),
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
  }

  @Test
  public void createTopic_topicExists() throws ConfigurationException {
    expectedException.expect(ConfigurationAlreadyExistsException.class);
    expectedException.expectMessage("Topic projects/project-1/topics/topic-1 already exists");

    com.google.pubsub.v1.Topic newTopic =
        com.google.pubsub.v1.Topic.newBuilder()
            .setName("projects/project-1/topics/topic-1")
            .build();
    configurationRepository.createTopic(newTopic);
  }

  @Test
  public void deleteTopic() throws ConfigurationException {
    configurationRepository.deleteTopic("projects/project-1/topics/topic-1");
    assertThat(
        configurationRepository.getTopics("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
  }

  @Test
  public void deleteTopic_topicDoesNotExist() throws ConfigurationException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage(
        "Topic projects/project-1/topics/does-not-exist does not exist");

    configurationRepository.deleteTopic("projects/project-1/topics/does-not-exist");
  }

  @Test
  public void createSubscription() throws ConfigurationException {
    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/new-subscription")
            .setTopic("projects/project-1/topics/topic-1")
            .build();
    configurationRepository.createSubscription(newSubscription);
    assertThat(
        configurationRepository.getSubscriptions("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/new-subscription")
                .setTopic("projects/project-1/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-3")
                .setTopic("projects/project-1/topics/topic-2")
                .setAckDeadlineSeconds(30)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-4")
                .setTopic("projects/project-1/topics/topic-2")
                .setAckDeadlineSeconds(45)
                .build()));
  }

  @Test
  public void createSubscription_subscriptionExists() throws ConfigurationException {
    expectedException.expect(ConfigurationAlreadyExistsException.class);
    expectedException.expectMessage(
        "Subscription projects/project-1/subscriptions/subscription-1 already exists");

    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/subscription-1")
            .setTopic("projects/project-1/topics/topic-1")
            .build();
    configurationRepository.createSubscription(newSubscription);
  }

  @Test
  public void createSubscription_topicDoesNotExist() throws ConfigurationException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage("Topic projects/project-1/topics/topic-10 does not exist");

    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/new-subscription")
            .setTopic("projects/project-1/topics/topic-10")
            .build();
    configurationRepository.createSubscription(newSubscription);
  }

  @Test
  public void deleteSubscription() throws ConfigurationException {
    configurationRepository.deleteSubscription("projects/project-2/subscriptions/subscription-3");
    assertThat(
        configurationRepository.getSubscriptions("projects/project-2"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-1")
                .setTopic("projects/project-2/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-2")
                .setTopic("projects/project-2/topics/topic-1")
                .setAckDeadlineSeconds(10)
                .build()));
  }

  @Test
  public void deleteSubscription_subscriptionDoesNotExist() throws ConfigurationException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage(
        "Subscription projects/project-2/subscriptions/does-not-exist does not exist");

    configurationRepository.deleteSubscription("projects/project-2/subscriptions/does-not-exist");
    assertThat(configurationRepository.getSubscriptions("projects/project-1"), Matchers.empty());
  }

  private static class TestConfigurationRepository extends ConfigurationRepository {

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

    private TestConfigurationRepository() {
      setConfiguration(CONFIGURATION);
    }

    @Override
    public Configuration load() {
      return null;
    }

    @Override
    public void save() {}
  }
}
