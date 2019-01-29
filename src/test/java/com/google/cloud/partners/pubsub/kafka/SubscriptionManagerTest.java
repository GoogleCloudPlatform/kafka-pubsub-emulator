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

package com.google.cloud.partners.pubsub.kafka;

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.KAFKA_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionManagerTest {

  private static final String KAFKA_TOPIC_NAME = "kafka-topic-1";
  private static final Subscription SUBSCRIPTION =
      Subscription.newBuilder()
          .setName("projects/project-1/subscriptions/subscription-manager-test")
          .setTopic(TestHelpers.PROJECT1_TOPIC1)
          .putLabels(KAFKA_TOPIC, KAFKA_TOPIC_NAME)
          .setAckDeadlineSeconds(10)
          .build();
  private static final Instant FIXED_INSTANT = Instant.ofEpochSecond(1546300800);

  @Mock private Clock mockClock;
  private MockKafkaClientFactory kafkaClientFactory;
  private SubscriptionManager subscriptionManager;
  private ScheduledExecutorService scheduledExecutor;

  @Before
  public void setUp() {
    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    kafkaClientFactory = new MockKafkaClientFactory();
    kafkaClientFactory.configureConsumersForSubscription(
        KAFKA_TOPIC_NAME, SUBSCRIPTION.getName(), 3, 0L, 0L);
    when(mockClock.instant()).thenReturn(FIXED_INSTANT);

    subscriptionManager = new SubscriptionManager(SUBSCRIPTION, kafkaClientFactory, mockClock, 4);
    subscriptionManager.startAsync().awaitRunning();
  }

  @After
  public void tearDown() {
    if (subscriptionManager.isRunning()) {
      subscriptionManager.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void stopAsync() {
    subscriptionManager.stopAsync().awaitTerminated();
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION.getName())
        .forEach(c -> assertTrue(c.closed()));
  }

  @Test
  public void pull() {
    int partitions = 3;
    int recordsPerPartition = 2;
    generateTestRecordsForConsumers(partitions, recordsPerPartition, null);

    // Each response should pull from a different partition
    List<String> messageIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();
    List<PubsubMessage> response = subscriptionManager.pull(10, false);
    for (PubsubMessage message : response) {
      messageIds.add(message.getMessageId());
      messages.add(message.getData().toStringUtf8());
    }
    response = subscriptionManager.pull(10, false);
    for (PubsubMessage message : response) {
      messageIds.add(message.getMessageId());
      messages.add(message.getData().toStringUtf8());
    }
    response = subscriptionManager.pull(10, false);
    for (PubsubMessage message : response) {
      messageIds.add(message.getMessageId());
      messages.add(message.getData().toStringUtf8());
    }

    assertThat(messageIds, Matchers.contains("0-0", "0-1", "1-0", "1-1", "2-0", "2-1"));
    assertThat(
        messages,
        Matchers.contains(
            "message-0000",
            "message-0001",
            "message-0002",
            "message-0003",
            "message-0004",
            "message-0005"));

    assertThat(subscriptionManager.pull(10, false), Matchers.empty());
  }

  @Test
  public void pull_withHeader() {
    int partitions = 1;
    int recordsPerPartition = 3;
    List<Header> headers = new ArrayList<>();
    headers.add(new RecordHeader("key1", "value1".getBytes()));
    headers.add(new RecordHeader("key2", "value2".getBytes()));
    generateTestRecordsForConsumers(partitions, recordsPerPartition, headers);

    // Each response should pull from a different partition
    List<String> messageIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();
    List<Map<String, String>> attributes = new ArrayList<>();
    List<PubsubMessage> response = subscriptionManager.pull(10, false);
    for (PubsubMessage message : response) {
      messageIds.add(message.getMessageId());
      messages.add(message.getData().toStringUtf8());
      attributes.add(message.getAttributesMap());
    }

    assertThat(messageIds, Matchers.contains("0-0", "0-1", "0-2"));
    assertThat(messages, Matchers.contains("message-0000", "message-0001", "message-0002"));
    ImmutableMap<String, String> expectedAttributes =
        new Builder<String, String>().put("key1", "value1").put("key2", "value2").build();
    assertThat(
        attributes,
        Matchers.equalTo(
            Arrays.asList(expectedAttributes, expectedAttributes, expectedAttributes)));

    assertThat(subscriptionManager.pull(10, false), Matchers.empty());
  }

  @Test
  public void pull_empty() {
    List<PubsubMessage> response = subscriptionManager.pull(100, false);
    assertTrue(response.isEmpty());
  }

  @Test
  public void acknowledge() {
    int partitions = 3;
    int recordsPerPartition = 2;
    generateTestRecordsForConsumers(partitions, recordsPerPartition, null);

    List<String> ackIds =
        subscriptionManager
            .pull(10, false)
            .stream()
            .map(PubsubMessage::getMessageId)
            .collect(Collectors.toList());
    assertEquals(ackIds, subscriptionManager.acknowledge(ackIds));

    subscriptionManager.runOneIteration();

    // Commits should only go to first partition
    TopicPartition topicPartition = new TopicPartition(KAFKA_TOPIC_NAME, 0);
    assertEquals(
        2,
        kafkaClientFactory
            .getConsumersForSubscription(SUBSCRIPTION.getName())
            .get(0)
            .position(topicPartition));

    for (int i = 1; i < partitions; i++) {
      topicPartition = new TopicPartition(KAFKA_TOPIC_NAME, i);
      assertEquals(
          0,
          kafkaClientFactory
              .getConsumersForSubscription(SUBSCRIPTION.getName())
              .get(i)
              .position(topicPartition));
    }
  }

  @Test
  public void acknowledge_unknownAckIds() {
    assertEquals(
        Collections.emptyList(),
        subscriptionManager.acknowledge(Arrays.asList("0-0", "0-1", "0-2")));
  }

  @Test
  public void acknowledge_expired() {
    int partitions = 3;
    int recordsPerPartition = 2;
    generateTestRecordsForConsumers(partitions, recordsPerPartition, null);

    // Pull 3 times to get all records
    List<String> ackIds = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      for (PubsubMessage message : subscriptionManager.pull(10, false)) {
        ackIds.add(message.getMessageId());
      }
    }
    when(mockClock.instant()).thenReturn(Instant.now());

    assertThat(subscriptionManager.acknowledge(ackIds), Matchers.empty());

    subscriptionManager.runOneIteration();

    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(KAFKA_TOPIC_NAME, i);
      assertEquals(
          0,
          kafkaClientFactory
              .getConsumersForSubscription(SUBSCRIPTION.getName())
              .get(i)
              .position(topicPartition));
    }
  }

  @Test
  public void modifyAckDeadline() {
    int partitions = 3;
    int recordsPerPartition = 2;
    generateTestRecordsForConsumers(partitions, recordsPerPartition, null);

    // Pull 3 times to get all records
    List<String> ackIds = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      for (PubsubMessage message : subscriptionManager.pull(10, false)) {
        ackIds.add(message.getMessageId());
      }
    }
    assertEquals(ackIds, subscriptionManager.modifyAckDeadline(ackIds, 10));

    // Should be after the initial expiration
    when(mockClock.instant()).thenReturn(FIXED_INSTANT.plusSeconds(15));

    assertEquals(ackIds, subscriptionManager.acknowledge(ackIds));

    subscriptionManager.runOneIteration();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(KAFKA_TOPIC_NAME, i);
      assertEquals(
          2,
          kafkaClientFactory
              .getConsumersForSubscription(SUBSCRIPTION.getName())
              .get(i)
              .position(topicPartition));
    }
  }

  @Test
  public void modifyAckDeadline_expired() {
    int partitions = 3;
    int recordsPerPartition = 2;
    generateTestRecordsForConsumers(partitions, recordsPerPartition, null);

    // Pull 3 times to get all records
    List<String> ackIds = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      for (PubsubMessage message : subscriptionManager.pull(10, false)) {
        ackIds.add(message.getMessageId());
      }
    }
    when(mockClock.instant()).thenReturn(Instant.now());

    assertEquals(Collections.emptyList(), subscriptionManager.modifyAckDeadline(ackIds, 10));
    assertEquals(Collections.emptyList(), subscriptionManager.acknowledge(ackIds));

    subscriptionManager.runOneIteration();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(KAFKA_TOPIC_NAME, i);
      assertEquals(
          0,
          kafkaClientFactory
              .getConsumersForSubscription(SUBSCRIPTION.getName())
              .get(i)
              .position(topicPartition));
    }
  }

  private void generateTestRecordsForConsumers(
      int partitions, int recordsPerPartition, List<Header> headers) {
    List<MockConsumer<String, ByteBuffer>> consumers =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION.getName());
    TestHelpers.generateConsumerRecords(KAFKA_TOPIC_NAME, partitions, recordsPerPartition, headers)
        .forEach(cr -> consumers.get(cr.partition() % consumers.size()).addRecord(cr));
  }
}
