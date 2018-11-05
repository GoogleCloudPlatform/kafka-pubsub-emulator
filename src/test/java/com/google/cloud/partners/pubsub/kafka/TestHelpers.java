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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import com.google.cloud.partners.pubsub.kafka.properties.ApplicationProperties;
import com.google.cloud.partners.pubsub.kafka.properties.PubSubBindProperties;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

public class TestHelpers {

  public static final Charset UTF8 = Charset.forName("UTF-8");
  public static final String SUBSCRIPTION1 = "subscription-1-to-test-topic-1";
  public static final String SUBSCRIPTION2 = "subscription-2-to-test-topic-1";
  public static final String SUBSCRIPTION3 = "subscription-1-to-test-topic-2";
  public static final String SUBSCRIPTION_NOT_EXISTS = "non-existent-subscription";
  public static final String SUBSCRIPTION_TO_DELETE = "subscription-to-delete";
  public static final String TOPIC1 = "test-topic-1";
  public static final String TOPIC2 = "test-topic-2";
  public static final String TOPIC_TO_DELETE = "topic-to-delete";
  public static final String TOPIC_NOT_EXISTS = "non-existent-topic";
  protected static final String PROJECT = "cpe-ti";
  private static final String CONFIGURATION_UNIT_TEST =
      "src/test/resources/application-unit-test.yaml";

  /** Sets application Configuration for unit tests. */
  public static void useTestApplicationConfig(int producers, int consumersPerSubscription) {
    Configuration.loadApplicationProperties(CONFIGURATION_UNIT_TEST);
    ApplicationProperties applicationConfig = Configuration.getApplicationProperties();
    applicationConfig
        .getKafkaProperties()
        .getConsumerProperties()
        .setExecutors(consumersPerSubscription);
    applicationConfig.getKafkaProperties().getProducerProperties().setExecutors(producers);
  }

  /** Generate a sequence of PubsubMessage objects. */
  public static List<PubsubMessage> generatePubsubMessages(int howMany) {
    List<PubsubMessage> messages = new ArrayList<>();
    for (int i = 0; i < howMany; i++) {
      messages.add(
          PubsubMessage.newBuilder().setData(ByteString.copyFrom("message-" + i, UTF8)).build());
    }
    return messages;
  }

  public static List<PubsubMessage> generatePubsubMessagesWithHeader(int howMany) {
    List<PubsubMessage> messages = new ArrayList<>();
    for (int i = 0; i < howMany; i++) {
      messages.add(
          PubsubMessage.newBuilder()
              .setData(ByteString.copyFrom("message-" + i, UTF8))
              .putAllAttributes(generateAttributes())
              .build());
    }
    return messages;
  }

  private static Map<String, String> generateAttributes() {
    Map<String, String> attributesMap = new HashMap<>();
    attributesMap.put("some-key", "some-value");
    return attributesMap;
  }

  /**
   * Generate a sequence of ConsumerRecord objects. The records will be evenly distributed amongst
   * the partitions in round-robin fashion.
   */
  public static List<ConsumerRecord<String, ByteBuffer>> generateConsumerRecords(
      String topic, int partitions, int recordsPerPartition, List<Header> headers) {
    List<ConsumerRecord<String, ByteBuffer>> records = new ArrayList<>();
    int messageSeq = 0;
    long now = System.currentTimeMillis();
    for (int p = 0; p < partitions; p++) {
      for (int r = 0; r < recordsPerPartition; r++) {
        ByteBuffer value =
            ByteBuffer.wrap(String.format("message-%04d", messageSeq++).getBytes(UTF8));
        records.add(
            new ConsumerRecord<>(
                topic,
                p,
                r,
                now,
                TimestampType.LOG_APPEND_TIME,
                (long) ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                value.capacity(),
                null,
                value,
                new RecordHeaders(headers)));
      }
    }
    return records;
  }

  protected static void setupRequestBindConfiguration() {
    Map<String, List<PubSubBindProperties>> pubSubProperties = newHashMap();
    pubSubProperties.put(PROJECT, newArrayList(givenPubSubBindProperty(SUBSCRIPTION1, TOPIC2)));
    Configuration.getApplicationProperties().setPubSubProperties(pubSubProperties);
  }

  private static PubSubBindProperties givenPubSubBindProperty(String subscription, String topic) {
    PubSubBindProperties property = new PubSubBindProperties();
    property.setSubscription(subscription);
    property.setTopic(topic);
    return property;
  }

  protected static void resetRequestBindConfiguration() {
    Configuration.getApplicationProperties().setPubSubProperties(newHashMap());
  }
}
