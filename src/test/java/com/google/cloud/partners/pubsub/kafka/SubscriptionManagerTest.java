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

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.pubsub.v1.PubsubMessage;

public class SubscriptionManagerTest {

  private static final String SUBSCRIPTION = "test-subscription";
  private static final String TOPIC = "test-topic";

  private MockKafkaClientFactoryImpl kafkaClientFactory;
  private SubscriptionManager subscriptionManager;
  private ScheduledExecutorService scheduledExecutor;

  @Before
  public void setUp() {
    configureSubscriptionManager(1, 3, 10);
  }

  @Test
  public void shutdown() {
    subscriptionManager.shutdown();
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION)
        .forEach(c -> assertEquals(true, c.closed()));
  }

  @Test
  public void pullSingleConsumer() {
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    List<PubsubMessage> response = subscriptionManager.pull(10, false);
    assertEquals(6, response.size());

    // Sort messageIds and data for easier evaluation
    List<String> messageIds =
        response.stream().map(PubsubMessage::getMessageId).sorted().collect(Collectors.toList());
    List<String> messages =
        response
            .stream()
            .map(m -> m.getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList());

    assertEquals("0-0", messageIds.get(0));
    assertEquals("0-1", messageIds.get(1));
    assertEquals("1-0", messageIds.get(2));
    assertEquals("1-1", messageIds.get(3));
    assertEquals("2-0", messageIds.get(4));
    assertEquals("2-1", messageIds.get(5));

    assertEquals("message-0000", messages.get(0));
    assertEquals("message-0001", messages.get(1));
    assertEquals("message-0002", messages.get(2));
    assertEquals("message-0003", messages.get(3));
    assertEquals("message-0004", messages.get(4));
    assertEquals("message-0005", messages.get(5));
  }

  @Test
  public void pullSingleConsumerWithHeader() {
    int partitions = 3;
    int recordsPerPartition = 2;

    List<Header> headers = new ArrayList<>();
    headers.add(new RecordHeader("key1", "value1".getBytes()));
    headers.add(new RecordHeader("key2", "value2".getBytes()));

    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, headers)
        .forEach(mockConsumer::addRecord);

    List<PubsubMessage> response = subscriptionManager.pull(10, false);
    assertEquals(6, response.size());

    // Sort messageIds and data for easier evaluation
    List<String> messageIds =
        response.stream().map(PubsubMessage::getMessageId).sorted().collect(Collectors.toList());
    List<String> messages =
        response
            .stream()
            .map(m -> m.getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList());

    Map<String, Map<String, String>> attributesMaps =
        response
            .stream()
            .collect(toMap(PubsubMessage::getMessageId, PubsubMessage::getAttributesMap));

    assertEquals("0-0", messageIds.get(0));
    assertPubSubAttributesMap(attributesMaps, "0-0");

    assertEquals("0-1", messageIds.get(1));
    assertPubSubAttributesMap(attributesMaps, "0-1");

    assertEquals("1-0", messageIds.get(2));
    assertPubSubAttributesMap(attributesMaps, "1-0");

    assertEquals("1-1", messageIds.get(3));
    assertPubSubAttributesMap(attributesMaps, "1-1");

    assertEquals("2-0", messageIds.get(4));
    assertPubSubAttributesMap(attributesMaps, "2-0");

    assertEquals("2-1", messageIds.get(5));
    assertPubSubAttributesMap(attributesMaps, "2-1");

    assertEquals("message-0000", messages.get(0));
    assertEquals("message-0001", messages.get(1));
    assertEquals("message-0002", messages.get(2));
    assertEquals("message-0003", messages.get(3));
    assertEquals("message-0004", messages.get(4));
    assertEquals("message-0005", messages.get(5));
  }

  private void assertPubSubAttributesMap(
      Map<String, Map<String, String>> attributesMaps, String messageId) {
    assertEquals("value1", attributesMaps.get(messageId).get("key1"));
    assertEquals("value2", attributesMaps.get(messageId).get("key2"));
  }

  @Test
  public void pullMultipleConsumers() {
    int partitions = 3;
    int recordsPerPartition = 5;
    configureSubscriptionManager(3, partitions, 10);

    // Generate records to each Consumer based on the partitions they were assigned to
    List<ConsumerRecord<String, ByteBuffer>> consumerRecords =
        TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null);
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION)
        .forEach(
            c ->
                c.assignment()
                    .forEach(
                        tp ->
                            consumerRecords
                                .stream()
                                .filter(r -> r.partition() == tp.partition())
                                .forEach(c::addRecord)));

    // Each Pull should use a different Consumer which is subscribed to a single partition
    List<PubsubMessage> response = subscriptionManager.pull(10, false);
    // Sort messageIds and data for easier evaluation
    List<String> messageIds =
        response.stream().map(PubsubMessage::getMessageId).sorted().collect(Collectors.toList());
    List<String> messages =
        response
            .stream()
            .map(m -> m.getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(5, response.size());
    assertEquals("0-0", messageIds.get(0));
    assertEquals("0-1", messageIds.get(1));
    assertEquals("0-2", messageIds.get(2));
    assertEquals("0-3", messageIds.get(3));
    assertEquals("0-4", messageIds.get(4));

    assertEquals("message-0000", messages.get(0));
    assertEquals("message-0001", messages.get(1));
    assertEquals("message-0002", messages.get(2));
    assertEquals("message-0003", messages.get(3));
    assertEquals("message-0004", messages.get(4));

    response = subscriptionManager.pull(10, false);
    assertEquals(5, response.size());
    messageIds.addAll(
        response.stream().map(PubsubMessage::getMessageId).sorted().collect(Collectors.toList()));
    messages.addAll(
        response
            .stream()
            .map(m -> m.getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList()));
    assertEquals("1-0", messageIds.get(5));
    assertEquals("1-1", messageIds.get(6));
    assertEquals("1-2", messageIds.get(7));
    assertEquals("1-3", messageIds.get(8));
    assertEquals("1-4", messageIds.get(9));

    assertEquals("message-0005", messages.get(5));
    assertEquals("message-0006", messages.get(6));
    assertEquals("message-0007", messages.get(7));
    assertEquals("message-0008", messages.get(8));
    assertEquals("message-0009", messages.get(9));

    response = subscriptionManager.pull(10, false);
    assertEquals(5, response.size());
    messageIds.addAll(
        response.stream().map(PubsubMessage::getMessageId).sorted().collect(Collectors.toList()));
    messages.addAll(
        response
            .stream()
            .map(m -> m.getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList()));
    assertEquals("2-0", messageIds.get(10));
    assertEquals("2-1", messageIds.get(11));
    assertEquals("2-2", messageIds.get(12));
    assertEquals("2-3", messageIds.get(13));
    assertEquals("2-4", messageIds.get(14));

    assertEquals("message-0010", messages.get(10));
    assertEquals("message-0011", messages.get(11));
    assertEquals("message-0012", messages.get(12));
    assertEquals("message-0013", messages.get(13));
    assertEquals("message-0014", messages.get(14));
  }

  @Test
  public void pullEmpty() {
    List<PubsubMessage> response = subscriptionManager.pull(100, false);
    assertEquals(true, response.isEmpty());
  }

  @Test
  public void acknowledgeSuccessfully() {
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    List<String> ackIds =
        subscriptionManager
            .pull(10, false)
            .stream()
            .map(PubsubMessage::getMessageId)
            .collect(Collectors.toList());
    assertEquals(ackIds, subscriptionManager.acknowledge(ackIds));
    // Confirm that one scheduled future was present in the executor service
    assertEquals(1, scheduledExecutor.shutdownNow().size());
    Map<TopicPartition, OffsetAndMetadata> commits =
        subscriptionManager.commitFromAcknowledgments();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(TOPIC, i);
      assertEquals(2, commits.get(topicPartition).offset());
      assertEquals(2, mockConsumer.position(topicPartition));
    }
  }

  @Test
  public void acknowledgeRepeatedCallsUseSameFuture() {
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    // Ack each message separately
    subscriptionManager
        .pull(10, false)
        .stream()
        .map(PubsubMessage::getMessageId)
        .forEach(
            ackId ->
                assertEquals(
                    Collections.singletonList(ackId),
                    subscriptionManager.acknowledge(Collections.singletonList(ackId))));

    // Confirm that one scheduled future was present in the executor service
    assertEquals(1, scheduledExecutor.shutdownNow().size());
    Map<TopicPartition, OffsetAndMetadata> commits =
        subscriptionManager.commitFromAcknowledgments();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(TOPIC, i);
      assertEquals(2, commits.get(topicPartition).offset());
      assertEquals(2, mockConsumer.position(topicPartition));
    }
  }

  @Test
  public void acknowledgeUnknownAckIds() {
    assertEquals(
        Collections.emptyList(),
        subscriptionManager.acknowledge(Arrays.asList("0-0", "0-1", "0-2")));
  }

  @Test
  public void acknowledgeExpired() {
    int partitions = 3;
    int recordsPerPartition = 2;
    configureSubscriptionManager(1, partitions, -1);
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    List<String> ackIds =
        subscriptionManager
            .pull(10, false)
            .stream()
            .map(PubsubMessage::getMessageId)
            .collect(Collectors.toList());
    assertEquals(Collections.emptyList(), subscriptionManager.acknowledge(ackIds));
    assertEquals(0, scheduledExecutor.shutdownNow().size());
    Map<TopicPartition, OffsetAndMetadata> commits =
        subscriptionManager.commitFromAcknowledgments();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(TOPIC, i);
      assertEquals(0, commits.get(topicPartition).offset());
      assertEquals(0, mockConsumer.position(topicPartition));
    }
  }

  @Test
  public void modifyAckDeadlineSuccessfully() {
    int partitions = 3;
    int recordsPerPartition = 2;
    configureSubscriptionManager(1, partitions, 2);
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    List<String> ackIds =
        subscriptionManager
            .pull(10, false)
            .stream()
            .map(PubsubMessage::getMessageId)
            .collect(Collectors.toList());
    assertEquals(ackIds, subscriptionManager.modifyAckDeadline(ackIds, 10));

    // Ack in 2 seconds (past original expiration)
    try {
      scheduledExecutor
          .schedule(
              () -> {
                assertEquals(ackIds, subscriptionManager.acknowledge(ackIds));
                Map<TopicPartition, OffsetAndMetadata> commits =
                    subscriptionManager.commitFromAcknowledgments();
                for (int i = 0; i < partitions; i++) {
                  TopicPartition topicPartition = new TopicPartition(TOPIC, i);
                  assertEquals(2, commits.get(topicPartition).offset());
                  assertEquals(2, mockConsumer.position(topicPartition));
                }
              },
              2,
              TimeUnit.SECONDS)
          .get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void modifyAckDeadlineExpired() {
    int partitions = 3;
    int recordsPerPartition = 2;
    configureSubscriptionManager(1, partitions, -1);
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION).get(0);
    TestHelpers.generateConsumerRecords(TOPIC, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    List<String> ackIds =
        subscriptionManager
            .pull(10, false)
            .stream()
            .map(PubsubMessage::getMessageId)
            .collect(Collectors.toList());
    assertEquals(Collections.emptyList(), subscriptionManager.modifyAckDeadline(ackIds, 10));
    assertEquals(Collections.emptyList(), subscriptionManager.acknowledge(ackIds));
    assertEquals(0, scheduledExecutor.shutdownNow().size());
    Map<TopicPartition, OffsetAndMetadata> commits =
        subscriptionManager.commitFromAcknowledgments();
    for (int i = 0; i < partitions; i++) {
      TopicPartition topicPartition = new TopicPartition(TOPIC, i);
      assertEquals(0, commits.get(topicPartition).offset());
      assertEquals(0, mockConsumer.position(topicPartition));
    }
  }

  /**
   * Sets {@link this#subscriptionManager} with specified configuration parameters
   */
  private void configureSubscriptionManager(
      int consumersPerSubscription, int topicPartitions, int ackDeadlineSecs) {
    TestHelpers.useTestApplicationConfig(1, consumersPerSubscription);
    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    kafkaClientFactory = new MockKafkaClientFactoryImpl();
    kafkaClientFactory.configureConsumersForSubscription(
        TOPIC, SUBSCRIPTION, topicPartitions, 0L, 0L);
    SubscriptionProperties subscriptionProperties = new SubscriptionProperties();
    subscriptionProperties.setName(SUBSCRIPTION);
    subscriptionProperties.setTopic(TOPIC);
    subscriptionProperties.setAckDeadlineSeconds(ackDeadlineSecs);
    subscriptionManager =
        new SubscriptionManager(subscriptionProperties, kafkaClientFactory, scheduledExecutor);
  }
}
