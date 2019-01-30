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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

import com.google.cloud.partners.pubsub.kafka.integration.util.BaseIT;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.Test;

/**
 * Integration tests for {@link com.google.cloud.partners.pubsub.kafka.PubsubEmulatorServer} using
 * Cloud Pub/Sub veneer libraries.
 */
public class ClientLibraryIT extends BaseIT {

  private static final Logger LOGGER = Logger.getLogger(ClientLibraryIT.class.getName());
  private static final String SUBSCRIPTION = "subscription-to-publish-and-streaming-pull";

  /**
   * Verifies that a Publisher can publish messages and a Subscriber will receive exactly those same
   * messages.
   */
  @Test(timeout = 30000)
  public void publishAndStreamingPull() throws Exception {
    String messagePrefix = CLIENT_LIBRARY_TOPIC + System.currentTimeMillis() + "-";
    int messages = 5000;
    Set<String> messagesSet = new TreeSet<>();
    for (int i = 0; i < messages; i++) {
      messagesSet.add(messagePrefix + i);
    }
    CountDownLatch publisherCountDownLatch = new CountDownLatch(messages);
    CountDownLatch subscriberCountDownLatch = new CountDownLatch(messages);
    Set<String> publishedIds = new ConcurrentSkipListSet<>();
    Set<String> receivedIds = new ConcurrentSkipListSet<>();
    LongAdder duplicates = new LongAdder();
    Publisher publisher = getPublisher(CLIENT_LIBRARY_TOPIC);

    Subscriber subscriber =
        getSubscriber(
            SUBSCRIPTION,
            (message, consumer) -> {
              consumer.ack();
              if (receivedIds.contains(message.getMessageId())) {
                duplicates.increment();
              } else {
                receivedIds.add(message.getMessageId());
              }
              subscriberCountDownLatch.countDown();
            });

    for (String data : messagesSet) {
      publish(
          publisher,
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(data)).build(),
          (throwable) -> LOGGER.severe("Publisher Error " + throwable.getMessage()),
          (result) -> {
            publishedIds.add(result);
            publisherCountDownLatch.countDown();
          });
    }
    LOGGER.info(format("Waiting for %d messages to be published by Publisher", messages));
    publisherCountDownLatch.await();
    LOGGER.info("Shutting down Publisher");
    publisher.shutdown();
    assertEquals(messages, publishedIds.size());

    subscriber.startAsync();
    LOGGER.info(format("Waiting for %d messages to be received by Subscriber", messages));
    subscriberCountDownLatch.await();
    LOGGER.info("Shutting down Subscriber");
    // Shouldn't be necessary, but it seems to ensure all acks get sent
    Thread.sleep(2000);
    subscriber.stopAsync().awaitTerminated();
    // Shouldn't be necessary, but to ensure the timing of consumers information
    Thread.sleep(3000);
    assertEquals(receivedIds, publishedIds);
    assertEquals(0, duplicates.intValue());

    Consumer<String, ByteBuffer> validator =
        getValidationConsumer(CLIENT_LIBRARY_TOPIC, SUBSCRIPTION);
    validator
        .assignment()
        .forEach(
            tp -> {
              Long endOffset = validator.endOffsets(Collections.singleton(tp)).getOrDefault(tp, 0L);
              OffsetAndMetadata committed = validator.committed(tp);
              assertEquals(endOffset, Long.valueOf(committed.offset()));
            });
  }

  /**
   * Verifies the scenario where a message is not acknowledged and all of the subsequent messages
   * from that partition are resent after a period of time.
   */
  @Test(timeout = 60000)
  public void publishAndStreamingPullSkipAck() throws Exception {
    String messagePrefix = CLIENT_LIBRARY_TOPIC + System.currentTimeMillis() + "-";
    int messages = 5000;
    Set<String> messagesSet = new TreeSet<>();
    for (int i = 0; i < messages; i++) {
      messagesSet.add(messagePrefix + i);
    }
    CountDownLatch publisherCountDownLatch = new CountDownLatch(messages);
    CountDownLatch subscriberCountDownLatch = new CountDownLatch(1);
    Set<String> publishedIds = new ConcurrentSkipListSet<>();
    Set<String> resentIds = new ConcurrentSkipListSet<>();
    Map<String, Integer> receivedIds = new ConcurrentHashMap<>();
    Publisher publisher = getPublisher(CLIENT_LIBRARY_TOPIC);
    AtomicInteger receivedCounter = new AtomicInteger();
    AtomicInteger reReceivedCounter = new AtomicInteger();
    CompletableFuture<String> skippedAck = new CompletableFuture<>();

    Subscriber subscriber =
        getSubscriber(
            SUBSCRIPTION,
            (message, consumer) -> {
              // Miss a single ack after half the messages are received
              if (receivedCounter.incrementAndGet() == messages / 2) {
                consumer.nack();
                skippedAck.complete(message.getMessageId());
              } else {
                consumer.ack();
                if (skippedAck.isDone()
                    && resentIds.contains(message.getMessageId())
                    && reReceivedCounter.incrementAndGet() == resentIds.size()) {
                  subscriberCountDownLatch.countDown();
                }
              }
              if (!receivedIds.containsKey(message.getMessageId())) {
                receivedIds.put(message.getMessageId(), 1);
              } else {
                int current = receivedIds.get(message.getMessageId());
                receivedIds.put(message.getMessageId(), ++current);
              }
            });

    for (String data : messagesSet) {
      publish(
          publisher,
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(data)).build(),
          (throwable) -> LOGGER.log(Level.SEVERE, "Error on publisher", throwable),
          (result) -> {
            publishedIds.add(result);
            publisherCountDownLatch.countDown();
          });
    }
    LOGGER.info(format("Waiting for %d messages to be published by Publisher", messages));
    publisherCountDownLatch.await();
    LOGGER.info("Shutting down Publisher");
    publisher.shutdown();
    assertEquals(messages, publishedIds.size());

    subscriber.startAsync();
    LOGGER.info(format("Skipped acknowledgment for %s", skippedAck.get()));
    // Determine the set of messages that should be received twice
    String[] pieces = skippedAck.get().split("-");
    int partition = Integer.parseInt(pieces[0]);
    int offset = Integer.parseInt(pieces[1]);
    publishedIds
        .stream()
        .filter(
            m -> {
              String[] pieces2 = m.split("-");
              int p2 = Integer.parseInt(pieces2[0]);
              int o2 = Integer.parseInt(pieces2[1]);
              return p2 == partition && o2 >= offset;
            })
        .forEach(resentIds::add);
    LOGGER.info(format("Waiting to re-receive %d messages", resentIds.size()));
    subscriberCountDownLatch.await();
    LOGGER.info("Shutting down Subscriber");
    // Shouldn't be necessary, but it seems to ensure all acks get sent
    Thread.sleep(2000);
    subscriber.stopAsync().awaitTerminated();
    // Shouldn't be necessary, but to ensure the timing of consumers information
    Thread.sleep(3000);
    assertEquals(receivedIds.keySet(), publishedIds);
    receivedIds.forEach(
        (key, value) -> {
          if (resentIds.contains(key)) {
            assertEquals(2, value.intValue());
          } else {
            assertEquals(1, value.intValue());
          }
        });

    Consumer<String, ByteBuffer> validator =
        getValidationConsumer(CLIENT_LIBRARY_TOPIC, SUBSCRIPTION);
    validator
        .assignment()
        .forEach(
            tp -> {
              Long endOffset = validator.endOffsets(Collections.singleton(tp)).getOrDefault(tp, 0L);
              OffsetAndMetadata committed = validator.committed(tp);
              assertEquals(endOffset, Long.valueOf(committed.offset()));
            });
  }
}
