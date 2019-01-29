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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class MockKafkaClientFactory implements KafkaClientFactory {

  private final List<MockProducer<String, ByteBuffer>> createdProducers;
  private final Map<String, List<MockConsumer<String, ByteBuffer>>> createdConsumers;
  private final Map<String, MockConsumerConfiguration> consumerConfigurations;

  MockKafkaClientFactory() {
    createdProducers = new ArrayList<>();
    createdConsumers = new HashMap<>();
    consumerConfigurations = new HashMap<>();
  }

  List<MockProducer<String, ByteBuffer>> getCreatedProducers() {
    return createdProducers;
  }

  List<MockConsumer<String, ByteBuffer>> getConsumersForSubscription(String subscription) {
    return createdConsumers.get(subscription);
  }

  @Override
  public Consumer<String, ByteBuffer> createConsumer(String subscription) {
    MockConsumer<String, ByteBuffer> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    if (!createdConsumers.containsKey(subscription)) {
      createdConsumers.put(subscription, new ArrayList<>());
    }
    createdConsumers.get(subscription).add(consumer);

    MockConsumerConfiguration configuration = consumerConfigurations.get(subscription);
    if (configuration != null) {
      consumer.updatePartitions(configuration.topic, configuration.partitionInfoList);
      consumer.updateBeginningOffsets(configuration.startOffsets);
      consumer.updateEndOffsets(configuration.endOffsets);
    }
    return consumer;
  }

  @Override
  public Producer<String, ByteBuffer> createProducer() {
    MockProducer<String, ByteBuffer> producer = new MockProducer<>();
    createdProducers.add(producer);
    return producer;
  }

  void configureConsumersForSubscription(
      String topic, String subscription, int partitions, long startingOffset, long endingOffset) {
    consumerConfigurations.put(
        subscription,
        new MockConsumerConfiguration(topic, partitions, startingOffset, endingOffset));
  }

  /** Used to specify configurations that will be used when MockConsumers are created. */
  private static class MockConsumerConfiguration {

    private final String topic;
    private final List<PartitionInfo> partitionInfoList;
    private final List<TopicPartition> topicPartitionList;
    private final Map<TopicPartition, Long> startOffsets;
    private final Map<TopicPartition, Long> endOffsets;

    private MockConsumerConfiguration(
        String topic, int partitions, long startingOffset, long endOffset) {
      this.topic = topic;
      partitionInfoList = new ArrayList<>();
      topicPartitionList = new ArrayList<>();
      startOffsets = new HashMap<>();
      endOffsets = new HashMap<>();
      for (int i = 0; i < partitions; i++) {
        Node node = new Node(i, "localhost", 9092 + i);
        partitionInfoList.add(new PartitionInfo(topic, i, node, null, null));
        topicPartitionList.add(new TopicPartition(topic, i));
        startOffsets.put(topicPartitionList.get(i), startingOffset);
        endOffsets.put(topicPartitionList.get(i), endOffset);
      }
    }
  }
}
