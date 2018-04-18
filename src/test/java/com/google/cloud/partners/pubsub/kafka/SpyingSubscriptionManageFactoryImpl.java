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

import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;

/**
 * Implementation of a {@link SubscriptionManagerFactory} that creates Mockito spies that can have
 * their interactions verified.
 */
public class SpyingSubscriptionManageFactoryImpl implements SubscriptionManagerFactory {

  private final Map<String, SubscriptionManager> managerMap;

  public SpyingSubscriptionManageFactoryImpl() {
    managerMap = new HashMap<>();
  }

  public SubscriptionManager getForSubscription(String name) {
    return managerMap.get(name);
  }

  @Override
  public SubscriptionManager create(
      SubscriptionProperties subscription,
      KafkaClientFactory clientFactory,
      ScheduledExecutorService commitExecutor) {
    SubscriptionManager spy =
        spy(new SubscriptionManager(subscription, clientFactory, commitExecutor));
    managerMap.put(subscription.getName(), spy);
    return spy;
  }
}
