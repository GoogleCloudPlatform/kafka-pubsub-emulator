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

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.google.cloud.partners.pubsub.kafka.properties.KafkaProperties;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

/** Responsible to collect statistics information of publish/consumed messages on emulator. */
public class StatisticsManager {

  private static final Logger LOGGER = Logger.getLogger(StatisticsManager.class.getName());
  private final Map<String, String> subscriptionByTopic;
  private final Map<String, StatisticsInformation> publishInformationByTopic =
      new ConcurrentHashMap<>();
  private final Map<String, StatisticsInformation> subscriberInformationByTopic =
      new ConcurrentHashMap<>();
  private final Clock clock;

  public StatisticsManager(Clock clock) {
    this.clock = clock;
    KafkaProperties kafkaProperties = Configuration.getApplicationProperties().getKafkaProperties();

    subscriptionByTopic =
        kafkaProperties
            .getConsumerProperties()
            .getSubscriptions()
            .stream()
            .collect(toMap(SubscriptionProperties::getName, SubscriptionProperties::getTopic));

    kafkaProperties
        .getTopics()
        .forEach(
            topic -> {
              publishInformationByTopic.put(topic, new StatisticsInformation());
              subscriberInformationByTopic.put(topic, new StatisticsInformation());
            });
  }

  public Map<String, StatisticsInformation> getPublishInformationByTopic() {
    return publishInformationByTopic;
  }

  public Map<String, StatisticsInformation> getSubscriberInformationByTopic() {
    return subscriberInformationByTopic;
  }

  public void computePublish(String topic, ByteString messageData, long publishAt) {
    if (!publishInformationByTopic.containsKey(topic)) {
      LOGGER.info("Topic not found to compute publish information.");
      return;
    }
    publishInformationByTopic
        .get(topic)
        .compute(clock.millis() - publishAt, messageData.toStringUtf8().length());
  }

  public void computePublishError(String topic) {
    if (!publishInformationByTopic.containsKey(topic)) {
      LOGGER.info("Topic not found to compute publish information.");
      return;
    }
    publishInformationByTopic.get(topic).computeError();
  }

  public void computeSubscriber(
      String subscription, ByteString messageData, Timestamp publishTime) {

    if (!subscriptionByTopic.containsKey(subscription)) {
      LOGGER.info("Subscription not found to compute subscriber information.");
      return;
    }

    String topic = subscriptionByTopic.get(subscription);
    Instant publishTimeToInstant =
        Instant.ofEpochSecond(publishTime.getSeconds(), publishTime.getNanos());
    long subscriptionLatency = clock.millis() - publishTimeToInstant.toEpochMilli();
    subscriberInformationByTopic
        .get(topic)
        .compute(subscriptionLatency, messageData.toStringUtf8().length());
  }

  public void addSubscriberInformation(SubscriptionProperties subscriptionProperties) {
    this.getSubscriberInformationByTopic()
        .putIfAbsent(subscriptionProperties.getTopic(), new StatisticsInformation());
  }

  public void removeSubscriberInformation(String topic) {
    this.getSubscriberInformationByTopic().remove(topic);
  }
}
