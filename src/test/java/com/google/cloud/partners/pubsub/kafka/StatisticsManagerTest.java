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

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class StatisticsManagerTest {

  public static final int DELTA = 0;
  private static final String TEST_TOPIC_1 = "test-topic-1";
  private static final String TEST_TOPIC_2 = "test-topic-2";
  private static final String SUBSCRIPTION_1_TEST_TOPIC_1 = "subscription-1-to-test-topic-1";
  private static final String SUBSCRIPTION_2_TEST_TOPIC_1 = "subscription-2-to-test-topic-1";
  private static final ByteString MESSAGE_DATA =
      ByteString.copyFrom(generateMessageContent(), Charset.forName("UTF-8"));
  private StatisticsManager statisticsManager;
  private Clock fixedClock;

  @BeforeClass
  public static void setUpBeforeClass() {
    TestHelpers.useTestApplicationConfig(1, 1);
  }

  private static String generateMessageContent() {
    StringBuilder stringBuffer = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      stringBuffer.append("A");
    }
    return stringBuffer.toString();
  }

  @Before
  public void setUp() {
    fixedClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    statisticsManager = new StatisticsManager(fixedClock);
  }

  @Test
  public void computePublisher() {
    long durationSeconds = 2L;

    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 10L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 535L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 50L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 59L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 19L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 3L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 11L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 13L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 931L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, fixedClock.millis() - 53L);
    statisticsManager.computePublish(TEST_TOPIC_2, MESSAGE_DATA, fixedClock.millis() - 90L);
    statisticsManager.computePublish(TEST_TOPIC_2, MESSAGE_DATA, fixedClock.millis() - 91L);

    Map<String, StatisticsInformation> publishInformation =
        statisticsManager.getPublishInformationByTopic();

    StatisticsInformation statisticsInformationForTopic1 = publishInformation.get(TEST_TOPIC_1);

    assertEquals(168.4F, statisticsInformationForTopic1.getAverageLatency(), DELTA);
    assertEquals(10, statisticsInformationForTopic1.getCount().intValue());
    assertEquals(25000F, statisticsInformationForTopic1.getThroughput(durationSeconds), DELTA);
    assertEquals(5F, statisticsInformationForTopic1.getQPS(durationSeconds), DELTA);
    assertEquals(0F, statisticsInformationForTopic1.getErrorRating(), DELTA);

    StatisticsInformation statisticsInformationForTopic2 = publishInformation.get(TEST_TOPIC_2);

    assertEquals(90.5F, statisticsInformationForTopic2.getAverageLatency(), DELTA);
    assertEquals(2, statisticsInformationForTopic2.getCount().intValue());
    assertEquals(5000F, statisticsInformationForTopic2.getThroughput(durationSeconds), DELTA);
    assertEquals(1F, statisticsInformationForTopic2.getQPS(durationSeconds), DELTA);
    assertEquals(0F, statisticsInformationForTopic2.getErrorRating(), DELTA);
  }

  @Test
  public void computePublisherError() {

    statisticsManager.computePublishError(TEST_TOPIC_1);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, 0L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, 0L);
    statisticsManager.computePublish(TEST_TOPIC_1, MESSAGE_DATA, 0L);

    assertEquals(
        25F,
        statisticsManager.getPublishInformationByTopic().get(TEST_TOPIC_1).getErrorRating(),
        DELTA);
  }

  @Test
  public void computePublisherErrorWithHundredPercent() {
    statisticsManager.computePublishError(TEST_TOPIC_1);

    assertEquals(
        100F,
        statisticsManager.getPublishInformationByTopic().get(TEST_TOPIC_1).getErrorRating(),
        DELTA);
    assertEquals(
        0F,
        statisticsManager.getPublishInformationByTopic().get(TEST_TOPIC_2).getErrorRating(),
        DELTA);
  }

  @Test
  public void computeSubscriber() {
    long durationSeconds = 3L;
    Instant now = fixedClock.instant();

    Instant receveivedMessage1 = now.minusMillis(200L);
    statisticsManager.computeSubscriber(
        SUBSCRIPTION_1_TEST_TOPIC_1,
        MESSAGE_DATA,
        Timestamp.newBuilder()
            .setSeconds(receveivedMessage1.getEpochSecond())
            .setNanos(receveivedMessage1.getNano())
            .build());

    Instant receveivedMessage2 = now.minusMillis(50L);
    statisticsManager.computeSubscriber(
        SUBSCRIPTION_2_TEST_TOPIC_1,
        MESSAGE_DATA,
        Timestamp.newBuilder()
            .setSeconds(receveivedMessage2.getEpochSecond())
            .setNanos(receveivedMessage2.getNano())
            .build());

    Instant receveivedMessage3 = now.minusMillis(120L);
    statisticsManager.computeSubscriber(
        SUBSCRIPTION_1_TEST_TOPIC_1,
        MESSAGE_DATA,
        Timestamp.newBuilder()
            .setSeconds(receveivedMessage3.getEpochSecond())
            .setNanos(receveivedMessage3.getNano())
            .build());

    Instant receveivedMessage4 = now.minusMillis(10L);
    statisticsManager.computeSubscriber(
        SUBSCRIPTION_1_TEST_TOPIC_1,
        MESSAGE_DATA,
        Timestamp.newBuilder()
            .setSeconds(receveivedMessage4.getEpochSecond())
            .setNanos(receveivedMessage4.getNano())
            .build());

    StatisticsInformation statisticsInformationForTopic1 =
        statisticsManager.getSubscriberInformationByTopic().get(TEST_TOPIC_1);

    assertEquals(95F, statisticsInformationForTopic1.getAverageLatency(), DELTA);
    assertEquals(4, statisticsInformationForTopic1.getCount().intValue());
    assertEquals(6666.66F, statisticsInformationForTopic1.getThroughput(durationSeconds), 0.1);
    assertEquals(1.33F, statisticsInformationForTopic1.getQPS(durationSeconds), 0.1);

    StatisticsInformation statisticsInformationForTopic2 =
        statisticsManager.getSubscriberInformationByTopic().get(TEST_TOPIC_2);

    assertEquals(0F, statisticsInformationForTopic2.getAverageLatency(), DELTA);
    assertEquals(0, statisticsInformationForTopic2.getCount().intValue());
    assertEquals(0F, statisticsInformationForTopic2.getThroughput(durationSeconds), DELTA);
    assertEquals(0F, statisticsInformationForTopic2.getQPS(durationSeconds), DELTA);
  }
}
