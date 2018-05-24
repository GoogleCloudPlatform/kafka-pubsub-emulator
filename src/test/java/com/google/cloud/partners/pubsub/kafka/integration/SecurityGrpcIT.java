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

package com.google.cloud.partners.pubsub.kafka.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.partners.pubsub.kafka.integration.util.BaseIT;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecurityGrpcIT extends BaseIT {

  private static final String TOPIC = "secured-server";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    USE_SSL = true;
    BaseIT.setUpBeforeClass();
  }

  @Test
  public void publishAndSubscribeOnSecureServer() throws Exception {
    // Sanity check
    assertTrue(USE_SSL);
    assertTrue(BaseIT.USE_SSL);

    SubscriptionProperties subscriptionProperties = getSubscriptionPropertiesByTopic(TOPIC);

    String messageData = "security-test-" + System.currentTimeMillis();

    CountDownLatch messageToPublish = new CountDownLatch(1);
    CountDownLatch messageToSubscribe = new CountDownLatch(1);

    List<String> publishedIds = new ArrayList<>();

    Map<String, Integer> receivedIds = new ConcurrentHashMap<>();
    Publisher publisher = getPublisher(subscriptionProperties);

    Subscriber subscriber =
        getSubscriber(
            subscriptionProperties,
            (message, consumer) -> {
              consumer.ack();
              if (messageData.equals(message.getData().toStringUtf8())) {
                receivedIds.put(message.getMessageId(), 1);
                messageToSubscribe.countDown();
              }
            });

    publish(
        publisher,
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(messageData)).build(),
        (throwable) -> messageToPublish.countDown(),
        (result) -> {
          publishedIds.add(result);
          messageToPublish.countDown();
        });

    messageToPublish.await(3, TimeUnit.SECONDS);
    publisher.shutdown();
    assertEquals(1, publishedIds.size());

    subscriber.startAsync();
    messageToSubscribe.await(3, TimeUnit.SECONDS);
    Thread.sleep(100);
    subscriber.stopAsync().awaitTerminated();
    assertTrue(publishedIds.containsAll(receivedIds.keySet()));
  }
}
