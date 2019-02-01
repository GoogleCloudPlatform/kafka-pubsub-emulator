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

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.FluentLogger;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Responsible to collect statistics information of publish/consumed messages on emulator. */
@Singleton
class StatisticsManager {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final ConfigurationRepository configurationRepository;
  private final Map<String, StatisticsInformation> publishInformationByTopic =
      new ConcurrentHashMap<>();
  private final Map<String, StatisticsInformation> subscriberInformationByTopic =
      new ConcurrentHashMap<>();
  private final Clock clock;

  @Inject
  StatisticsManager(ConfigurationRepository configurationRepository, Clock clock) {
    this.configurationRepository = configurationRepository;
    this.clock = clock;

    for (String project : configurationRepository.getProjects()) {
      for (Topic topic : configurationRepository.getTopics(project)) {
        publishInformationByTopic.put(topic.getName(), new StatisticsInformation());
        subscriberInformationByTopic.put(topic.getName(), new StatisticsInformation());
      }
    }
  }

  Map<String, StatisticsInformation> getPublishInformationByTopic() {
    return ImmutableMap.copyOf(publishInformationByTopic);
  }

  Map<String, StatisticsInformation> getSubscriberInformationByTopic() {
    return ImmutableMap.copyOf(subscriberInformationByTopic);
  }

  void computePublish(String topic, ByteString messageData, long publishAt) {
    if (!publishInformationByTopic.containsKey(topic)) {
      logger.atWarning().log("Unable to record publish statistics for Topic %s", topic);
    } else {
      publishInformationByTopic
          .get(topic)
          .compute(clock.millis() - publishAt, messageData.toStringUtf8().length());
    }
  }

  void computePublishError(String topic) {
    StatisticsInformation statisticsInformation = publishInformationByTopic.get(topic);
    if (statisticsInformation == null) {
      logger.atWarning().log("Unable to record publish error for Topic %s", topic);
      return;
    }
    statisticsInformation.computeError();
  }

  void computeSubscriber(String subscription, ByteString messageData, Timestamp publishTime) {
    Optional<Subscription> subscriptionByName =
        configurationRepository.getSubscriptionByName(subscription);
    if (subscriptionByName.isPresent()) {
      StatisticsInformation statisticsInformation =
          subscriberInformationByTopic.get(subscriptionByName.get().getTopic());
      if (statisticsInformation == null) {
        logger.atWarning().log(
            "Unable to record subscriber statistics error for Subscription %s (Topic %s not found)",
            subscription, subscriberInformationByTopic.get(subscriptionByName.get().getTopic()));
        return;
      }
      Instant publishTimeToInstant =
          Instant.ofEpochSecond(publishTime.getSeconds(), publishTime.getNanos());
      long subscriptionLatency = clock.millis() - publishTimeToInstant.toEpochMilli();
      statisticsInformation.compute(subscriptionLatency, messageData.toStringUtf8().length());
    } else {
      logger.atWarning().log(
          "Unable to record subscriber statistics error for Subscription %s", subscription);
    }
  }

  void addSubscriberInformation(Subscription subscription) {
    this.getSubscriberInformationByTopic()
        .putIfAbsent(subscription.getTopic(), new StatisticsInformation());
  }

  public void removeSubscriberInformation(String topic) {
    this.getSubscriberInformationByTopic().remove(topic);
  }
}
