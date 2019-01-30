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
import com.google.pubsub.v1.Subscription;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Factory for creating SubscriptionManagers. */
@Singleton
class SubscriptionManagerFactory {

  private final ConfigurationRepository configurationRepository;
  private final KafkaClientFactory kafkaClientFactory;

  @Inject
  SubscriptionManagerFactory(
      ConfigurationRepository configurationRepository, KafkaClientFactory kafkaClientFactory) {
    this.configurationRepository = configurationRepository;
    this.kafkaClientFactory = kafkaClientFactory;
  }

  public SubscriptionManager create(Subscription subscription) {
    return new SubscriptionManager(
        subscription,
        kafkaClientFactory,
        Clock.systemUTC(),
        configurationRepository.getKafka().getConsumersPerSubscription());
  }
}
