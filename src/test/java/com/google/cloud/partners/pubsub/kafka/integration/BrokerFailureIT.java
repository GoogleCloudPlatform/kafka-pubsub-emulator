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

import com.google.cloud.partners.pubsub.kafka.integration.util.BaseIT;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import org.junit.Test;

public class BrokerFailureIT extends BaseIT {

  private static final Logger LOGGER = Logger.getLogger(BrokerFailureIT.class.getName());
  private static final String TOPIC = "broker-failure";

  /**
   * Evaluate that a Publisher/Subscriber can survive a broker failure.
   */
  @Test(timeout = 300000)
  public void testBrokerUpDownUp() throws Exception {
    SubscriptionProperties subscriptionProperties = getSubscriptionPropertiesByTopic(TOPIC);

    ByteString message1 = ByteString.copyFromUtf8("message-1");
    ByteString message2 = ByteString.copyFromUtf8("message-2");
    ByteString messageError = ByteString.copyFromUtf8("message-error");

    CountDownLatch publish1 = new CountDownLatch(1);
    CountDownLatch publish2 = new CountDownLatch(1);
    CountDownLatch error = new CountDownLatch(1);
    CountDownLatch receive1 = new CountDownLatch(1);
    CountDownLatch receive2 = new CountDownLatch(1);

    Set<String> publishedIds = new ConcurrentSkipListSet<>();
    Set<String> receivedIds = new ConcurrentSkipListSet<>();

    Publisher publisher = getPublisher(subscriptionProperties);
    Subscriber subscriber =
        getSubscriber(
            subscriptionProperties,
            (message, consumer) -> {
              consumer.ack();
              receivedIds.add(message.getMessageId());
              if (message.getData().equals(message1)) {
                receive1.countDown();
              } else if (message.getData().equals(message2)) {
                receive2.countDown();
              }
            });
    subscriber.startAsync().awaitRunning();

    // First, publish and subscribe with everything working as expected
    publish(
        publisher,
        PubsubMessage.newBuilder().setData(message1).build(),
        (throwable) -> LOGGER.warning("Unexpected error during Publish"),
        (result) -> {
          publishedIds.add(result);
          publish1.countDown();
        });

    LOGGER.info("Awaiting successful Publish/Subscribe");
    publish1.await();
    receive1.await();
    assertEquals(1, publishedIds.size());
    assertEquals(publishedIds, receivedIds);

    // Shutdown and try to publish, which should fail
    LOGGER.info("Shutting down brokers...");
    for (int i = 0; i < KAFKA_RULE.getReplicationFactor(); i++) {
      KAFKA_RULE.shutdown(i);
    }

    publish(
        publisher,
        PubsubMessage.newBuilder().setData(messageError).build(),
        (throwable) -> error.countDown(),
        (result) -> LOGGER.warning("Unexpected successful Publish"));
    LOGGER.info("Awaiting error during Publish");
    error.await();

    LOGGER.info("Restarting brokers and waiting 30s...");
    for (int i = 0; i < KAFKA_RULE.getReplicationFactor(); i++) {
      KAFKA_RULE.start(i);
    }
    // This allows Kafka time to stablize topic leadership, if not, results can be tough to validate
    Thread.sleep(30000);

    publish(
        publisher,
        PubsubMessage.newBuilder().setData(message2).build(),
        (throwable) -> LOGGER.warning("Unexpected error during Publish"),
        (result) -> {
          publishedIds.add(result);
          publish2.countDown();
        });
    LOGGER.info("Awaiting successful Publish/Subscribe after restart");
    publish2.await();
    receive2.await();
    assertEquals(2, publishedIds.size());
    assertEquals(publishedIds, receivedIds);
    subscriber.stopAsync().awaitTerminated();
  }
}
