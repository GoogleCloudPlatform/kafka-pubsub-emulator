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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.threeten.bp.Duration;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.common.Metric;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsRequest;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsResponse;
import com.google.cloud.partners.pubsub.kafka.integration.util.BaseIT;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

/**
 * This integration test is designed to run against the emulator running at localhost:8080 and will
 * execute a series of Publish/Subscribe worker threads to evaluate the throughput and latency of
 * the server. The server's configuration is left up to the tester. By default, these tests are
 * ignored by JUnit and by Maven.
 */
@RunWith(Parameterized.class)
public class PerformanceBenchmarkIT {

  private static final Logger LOGGER = Logger.getLogger(PerformanceBenchmarkIT.class.getName());
  private static final int MAX_OUTSTANDING_PUBLISH_REQUESTS = 2000;
  private static final String MESSAGE_SEQUENCE = "messageSequence";
  private static final String SENT_AT = "sentAt";

  private final CredentialsProvider credentialsProvider;
  private final String topic;
  private final String subscription;
  private final int testDuration;
  private final int numPublishers;
  private final int numSubscribers;
  private final int messageSizeBytes;

  private final Map<String, Long> publishedLatencies;
  private final Map<String, Integer> receivedIds;
  private final LongAdder bytesPublished;
  private final LongAdder bytesReceived;
  private final LongAdder publishErrors;
  private final AdminGrpc.AdminBlockingStub adminBlockingStub;
  private Instant startedAt;

  /**
   * Constructor for parameterized test
   */
  public PerformanceBenchmarkIT(
      String topic, int testDuration, int numPublishers, int numSubscribers, int messageSizeBytes)
      throws IOException {
    this.topic = topic;
    this.subscription = "subscription-" + topic;
    this.testDuration = testDuration;
    this.numPublishers = numPublishers;
    this.numSubscribers = numSubscribers;
    this.messageSizeBytes = messageSizeBytes;

    credentialsProvider = new NoCredentialsProvider();
    publishedLatencies = new ConcurrentHashMap<>();
    receivedIds = new ConcurrentHashMap<>();
    bytesPublished = new LongAdder();
    bytesReceived = new LongAdder();
    publishErrors = new LongAdder();

    adminBlockingStub = BaseIT.getAdminStub();
  }

  @Parameterized.Parameters
  public static Collection benchmarkArguments() {
    return Arrays.asList(
        new Object[][]{
            {"performance-testing-8p", 60, 1, 1, 1000},
            {"performance-testing-8p", 60, 2, 2, 1000},
            {"performance-testing-8p", 60, 8, 8, 10},
            {"performance-testing-64p", 60, 2, 8, 100},
            {"performance-testing-64p", 60, 4, 16, 100},
            {"performance-testing-64p", 60, 16, 16, 10},
            {"performance-testing-128p", 60, 1, 1, 1000},
            {"performance-testing-128p", 60, 2, 2, 1000},
            {"performance-testing-128p", 60, 8, 8, 1000}
        });
  }

  @Test
  public void executeTest() throws Exception {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(numPublishers + 1);
    CountDownLatch publisherCountDown = new CountDownLatch(1);
    CountDownLatch subscriberCountDown = new CountDownLatch(1);

    List<Publisher> publishers = new ArrayList<>();
    List<Subscriber> subscribers = new ArrayList<>();
    LongAdder messageSequence = new LongAdder();
    ByteString message;

    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < messageSizeBytes; i++) {
      buffer.append("a");
    }
    message = ByteString.copyFromUtf8(buffer.toString());

    for (int i = 0; i < numPublishers; i++) {
      publishers.add(getPublisher());
    }
    for (int i = 0; i < numSubscribers; i++) {
      subscribers.add(getSubscriber(publisherCountDown, subscriberCountDown));
    }

    LOGGER.info(
        format(
            "Running throughput test on %s for %ds using %d Publishers and %d Subscribers using %d byte messages",
            topic, testDuration, publishers.size(), subscribers.size(), messageSizeBytes));
    startedAt = Instant.now();
    subscribers.forEach(Subscriber::startAsync);
    publishers.forEach(
        publisher ->
            executorService.submit(
                () -> {
                  Semaphore maxOutstandingRequests =
                      new Semaphore(MAX_OUTSTANDING_PUBLISH_REQUESTS);
                  boolean done = false;
                  while (!done) {
                    try {
                      maxOutstandingRequests.acquire();
                    } catch (InterruptedException ignored) {
                    }
                    long publishedAt = System.currentTimeMillis();
                    messageSequence.increment();
                    ApiFutures.addCallback(
                        publisher.publish(
                            PubsubMessage.newBuilder()
                                .setData(message)
                                .putAttributes(MESSAGE_SEQUENCE, messageSequence.toString())
                                .putAttributes(SENT_AT, String.valueOf(publishedAt))
                                .build()),
                        new ApiFutureCallback<String>() {
                          @Override
                          public void onFailure(Throwable throwable) {
                            publishErrors.increment();
                            maxOutstandingRequests.release();
                          }

                          @Override
                          public void onSuccess(String messageId) {
                            publishedLatencies.put(
                                messageId, System.currentTimeMillis() - publishedAt);
                            bytesPublished.add(messageSizeBytes);
                            maxOutstandingRequests.release();
                          }
                        });
                    try {
                      // 1M QPS per Publisher
                      done = publisherCountDown.await(1, TimeUnit.MICROSECONDS);
                    } catch (InterruptedException ignored) {
                    }
                  }
                }));
    // Schedule status updates at regular intervals
    executorService.scheduleAtFixedRate(this::summarizeResults, 10, 10, TimeUnit.SECONDS);

    try {
      publisherCountDown.await(testDuration, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
    publisherCountDown.countDown();
    LOGGER.info("Test complete, shutting down Publishers");
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    LOGGER.info("Waiting for up to 30s for all Published messages to be received by Subscribers");
    if (!subscriberCountDown.await(30, TimeUnit.SECONDS)) {
      LOGGER.warning("Not all messages were received by Subscribers");
    }
    LOGGER.info("Shutting down Subscribers");
    subscribers.forEach(s -> s.stopAsync().awaitTerminated());
    summarizeResults();
    executorService.shutdown();
  }

  private Subscriber getSubscriber(
      CountDownLatch publisherCountDown, CountDownLatch subscriberCountDown) {
    return Subscriber.newBuilder(
        subscription,
        (message, consumer) -> {
          consumer.ack();
          bytesReceived.add(messageSizeBytes);
          if (!receivedIds.containsKey(message.getMessageId())) {
            receivedIds.put(message.getMessageId(), 1);
          } else {
            int current = receivedIds.get(message.getMessageId());
            receivedIds.put(message.getMessageId(), ++current);
          }
          try {
            // If publishing is done but subscribing is not
            if (publisherCountDown.await(1, TimeUnit.NANOSECONDS)
                && !subscriberCountDown.await(1, TimeUnit.NANOSECONDS)
                && publishedLatencies.keySet().containsAll(receivedIds.keySet())) {
              subscriberCountDown.countDown();
            }
          } catch (InterruptedException ignored) {
          }
        })
        .setChannelProvider(BaseIT.getChannelProvider())
        .setCredentialsProvider(credentialsProvider)
        .build();
  }

  private Publisher getPublisher() throws IOException {
    return Publisher.newBuilder(topic)
        .setCredentialsProvider(credentialsProvider)
        .setChannelProvider(BaseIT.getChannelProvider())
        // Batching settings borrowed from PubSub Load Test Framework
        .setBatchingSettings(
            BatchingSettings.newBuilder()
                .setElementCountThreshold(950L)
                .setRequestByteThreshold(9500000L)
                .setDelayThreshold(Duration.ofMillis(10))
                .build())
        .build();
  }

  private void summarizeResults() {
    long durationSeconds = java.time.Duration.between(startedAt, Instant.now()).getSeconds();
    float publisherThroughput = bytesPublished.floatValue() / durationSeconds;
    float subscriberThroughput = bytesReceived.floatValue() / durationSeconds;
    List<Long> sortedPublishLatencies =
        publishedLatencies.values().stream().sorted().collect(Collectors.toList());
    Double avgPublishLatency =
        sortedPublishLatencies.stream().collect(Collectors.averagingLong(Long::longValue));
    long duplicates = receivedIds.values().stream().filter(v -> v > 1).count();

    StatisticsResponse statistics =
        adminBlockingStub.statistics(StatisticsRequest.newBuilder().build());
    String publisherMetrics =
        statistics
            .getPublisherByTopicMap()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().equals(topic))
            .flatMap(e -> e.getValue().getMetricsList().stream())
            .sorted(Comparator.comparing(Metric::getName))
            .map(m -> format("%s: %s", m.getName(), m.getValue()))
            .collect(Collectors.joining(", "));
    String subscriberMetrics =
        statistics
            .getSubscriberByTopicMap()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().equals(topic))
            .flatMap(e -> e.getValue().getMetricsList().stream())
            .sorted(Comparator.comparing(Metric::getName))
            .map(m -> format("%s: %s", m.getName(), m.getValue()))
            .collect(Collectors.joining(", "));

    LOGGER.info(format("After %ds", durationSeconds));
    LOGGER.info(
        format(
            "Published %d messages, %3.2f Mb (%3.2f Mb/s) with %d errors",
            publishedLatencies.size(),
            bytesPublished.floatValue() / 1000000,
            publisherThroughput / 1000000,
            publishErrors.longValue()));
    LOGGER.info(
        format(
            "Received %d messages, %3.2f Mb (%3.2f Mb/s) with %d duplicates",
            receivedIds.size(),
            bytesReceived.floatValue() / 1000000,
            subscriberThroughput / 1000000,
            duplicates));
    LOGGER.info(
        format(
            "Avg Publish Latency %3.2fms at %3.2f qps",
            avgPublishLatency, publishedLatencies.size() / (float) durationSeconds));

    LOGGER.info("Server-captured Statistics");
    LOGGER.info(format("--- Publisher Metrics ---\n%s", publisherMetrics));
    LOGGER.info(format("--- Subscriber Metrics ---\n%s", subscriberMetrics));
  }
}
