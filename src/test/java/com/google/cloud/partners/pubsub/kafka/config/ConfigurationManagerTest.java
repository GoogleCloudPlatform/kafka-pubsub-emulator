package com.google.cloud.partners.pubsub.kafka.config;

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager.KAFKA_TOPIC;
import static org.junit.Assert.assertThat;

import com.google.cloud.partners.pubsub.kafka.TestHelpers;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager.ConfigurationAlreadyExistsException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager.ConfigurationNotFoundException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConfigurationManagerTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();
  private FakePubSubRepository fakePubSubRepository = new FakePubSubRepository();
  private ConfigurationManager configurationManager;

  @Before
  public void setUp() {
    configurationManager =
        new ConfigurationManager(TestHelpers.SERVER_CONFIG, fakePubSubRepository);
  }

  @Test
  public void getProjects() {
    assertThat(
        configurationManager.getProjects(),
        Matchers.contains("projects/project-1", "projects/project-2"));
  }

  @Test
  public void getTopics() {
    assertThat(
        configurationManager.getTopics("projects/project-1"),
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
        configurationManager.getTopics("projects/project-2"),
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
    assertThat(configurationManager.getTopics("projects/missing"), Matchers.empty());
  }

  @Test
  public void getSubscriptions() {
    assertThat(
        configurationManager.getSubscriptions("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-3")
                .setTopic("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .setAckDeadlineSeconds(30)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-4")
                .setTopic("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .setAckDeadlineSeconds(45)
                .build()));
    assertThat(
        configurationManager.getSubscriptions("projects/project-2"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-1")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-2")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-3")
                .setTopic("projects/project-2/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .setAckDeadlineSeconds(30)
                .build()));
  }

  @Test
  public void getSubscriptions_projectNotFound() {
    assertThat(configurationManager.getSubscriptions("projects/missing"), Matchers.empty());
  }

  @Test
  public void getSubscriptionsForTopic() {
    assertThat(
        configurationManager.getSubscriptionsForTopic("projects/project-1/topics/topic-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build()));
    assertThat(
        configurationManager.getSubscriptionsForTopic("projects/project-2/topics/topic-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-1")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-2")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build()));
  }

  @Test
  public void createTopic() throws ConfigurationAlreadyExistsException {
    com.google.pubsub.v1.Topic newTopic =
        com.google.pubsub.v1.Topic.newBuilder()
            .setName("projects/project-1/topics/a-new-topic")
            .build();
    configurationManager.createTopic(newTopic);
    assertThat(
        configurationManager.getTopics("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/a-new-topic")
                .putLabels(KAFKA_TOPIC, "project-1_a-new-topic")
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
  public void createTopic_topicExists() throws ConfigurationAlreadyExistsException {
    expectedException.expect(ConfigurationAlreadyExistsException.class);
    expectedException.expectMessage("Topic projects/project-1/topics/topic-1 already exists");

    com.google.pubsub.v1.Topic newTopic =
        com.google.pubsub.v1.Topic.newBuilder()
            .setName("projects/project-1/topics/topic-1")
            .build();
    configurationManager.createTopic(newTopic);
  }

  @Test
  public void deleteTopic() throws ConfigurationNotFoundException {
    configurationManager.deleteTopic("projects/project-1/topics/topic-1");
    assertThat(
        configurationManager.getTopics("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
  }

  @Test
  public void deleteTopic_topicDoesNotExist() throws ConfigurationNotFoundException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage(
        "Topic projects/project-1/topics/does-not-exist does not exist");

    configurationManager.deleteTopic("projects/project-1/topics/does-not-exist");
  }

  @Test
  public void createSubscription()
      throws ConfigurationNotFoundException, ConfigurationAlreadyExistsException {
    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/new-subscription")
            .setTopic("projects/project-1/topics/topic-1")
            .build();
    configurationManager.createSubscription(newSubscription);
    assertThat(
        configurationManager.getSubscriptions("projects/project-1"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/new-subscription")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-3")
                .setTopic("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .setAckDeadlineSeconds(30)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-4")
                .setTopic("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .setAckDeadlineSeconds(45)
                .build()));
  }

  @Test
  public void createSubscription_subscriptionExists()
      throws ConfigurationNotFoundException, ConfigurationAlreadyExistsException {
    expectedException.expect(ConfigurationAlreadyExistsException.class);
    expectedException.expectMessage(
        "Subscription projects/project-1/subscriptions/subscription-1 already exists");

    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/subscription-1")
            .setTopic("projects/project-1/topics/topic-1")
            .build();
    configurationManager.createSubscription(newSubscription);
  }

  @Test
  public void createSubscription_topicDoesNotExist()
      throws ConfigurationNotFoundException, ConfigurationAlreadyExistsException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage("Topic projects/project-1/topics/topic-10 does not exist");

    com.google.pubsub.v1.Subscription newSubscription =
        com.google.pubsub.v1.Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/new-subscription")
            .setTopic("projects/project-1/topics/topic-10")
            .build();
    configurationManager.createSubscription(newSubscription);
  }

  @Test
  public void deleteSubscription() throws ConfigurationNotFoundException {
    configurationManager.deleteSubscription("projects/project-2/subscriptions/subscription-3");
    assertThat(
        configurationManager.getSubscriptions("projects/project-2"),
        Matchers.contains(
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-1")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            com.google.pubsub.v1.Subscription.newBuilder()
                .setName("projects/project-2/subscriptions/subscription-2")
                .setTopic("projects/project-2/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build()));
  }

  @Test
  public void deleteSubscription_subscriptionDoesNotExist() throws ConfigurationNotFoundException {
    expectedException.expect(ConfigurationNotFoundException.class);
    expectedException.expectMessage(
        "Subscription projects/project-2/subscriptions/does-not-exist does not exist");

    configurationManager.deleteSubscription("projects/project-2/subscriptions/does-not-exist");
    assertThat(configurationManager.getSubscriptions("projects/project-1"), Matchers.empty());
  }
}
