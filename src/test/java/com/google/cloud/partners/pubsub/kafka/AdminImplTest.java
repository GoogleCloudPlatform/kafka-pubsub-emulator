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

import static com.google.cloud.partners.pubsub.kafka.enums.MetricProperty.AVG_LATENCY;
import static com.google.cloud.partners.pubsub.kafka.enums.MetricProperty.ERROR_RATE;
import static com.google.cloud.partners.pubsub.kafka.enums.MetricProperty.MESSAGE_COUNT;
import static com.google.cloud.partners.pubsub.kafka.enums.MetricProperty.QPS;
import static com.google.cloud.partners.pubsub.kafka.enums.MetricProperty.THROUGHPUT;
import static java.lang.Float.parseFloat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc.AdminBlockingStub;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationRequest;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse.Extension;
import com.google.cloud.partners.pubsub.kafka.common.Metric;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsConsolidation;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsRequest;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsResponse;
import io.grpc.testing.GrpcServerRule;

@RunWith(MockitoJUnitRunner.class)
public class AdminImplTest {

  private static final String TEST_TOPIC_1 = "test-topic-1";
  private static final String TEST_TOPIC_2 = "test-topic-2";

  private static final int EXECUTOR_PRODUCER = 1;

  private static final int EXECUTOR_SUBSCRIBER = 1;
  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();
  private AdminBlockingStub blockingStub;
  private AdminImpl admin;
  @Mock private StatisticsManager statisticsManager;

  @BeforeClass
  public static void setUpBeforeClass() {
    TestHelpers.useTestApplicationConfig(EXECUTOR_PRODUCER, EXECUTOR_SUBSCRIBER);
  }

  @Before
  public void setUp() {
    admin = new AdminImpl(statisticsManager);
    grpcServerRule.getServiceRegistry().addService(admin);
    blockingStub = AdminGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void statistics() throws Exception {

    when(statisticsManager.getPublishInformationByTopic()).thenReturn(givenPublishInformation());

    when(statisticsManager.getSubscriberInformationByTopic())
        .thenReturn(givenSubscriberInformation());

    Thread.sleep(5000L);

    StatisticsResponse statisticsResponse =
        blockingStub.statistics(StatisticsRequest.newBuilder().build());

    assertEquals(EXECUTOR_SUBSCRIBER, statisticsResponse.getSubscriberExecutors());
    assertEquals(EXECUTOR_PRODUCER, statisticsResponse.getPublisherExecutors());

    float deltaForLatency = 10F;
    float deltaForThroughput = 100F;
    float deltaForQPS = 1F;
    int noDelta = 0;

    // assert information for publisher of topic 1
    Map<String, String> publisherMetricsForTopic1 =
        convertToMetricMap(statisticsResponse.getPublisherByTopicMap().get(TEST_TOPIC_1));
    assertEquals(10, Integer.parseInt(publisherMetricsForTopic1.get(MESSAGE_COUNT.getName())));
    assertEquals(
        76.1F, parseFloat(publisherMetricsForTopic1.get(AVG_LATENCY.getName())), deltaForLatency);
    assertEquals(
        10000.0F,
        parseFloat(publisherMetricsForTopic1.get(THROUGHPUT.getName())),
        deltaForThroughput);
    assertEquals(2.0F, parseFloat(publisherMetricsForTopic1.get(QPS.getName())), deltaForQPS);
    assertEquals(9.09F, parseFloat(publisherMetricsForTopic1.get(ERROR_RATE.getName())), noDelta);

    // assert information for publisher of topic 2
    Map<String, String> publisherMetricsForTopic2 =
        convertToMetricMap(statisticsResponse.getPublisherByTopicMap().get(TEST_TOPIC_2));

    assertEquals(2, Integer.parseInt(publisherMetricsForTopic2.get(MESSAGE_COUNT.getName())));
    assertEquals(
        90F, parseFloat(publisherMetricsForTopic2.get(AVG_LATENCY.getName())), deltaForLatency);
    assertEquals(
        2000F, parseFloat(publisherMetricsForTopic2.get(THROUGHPUT.getName())), deltaForThroughput);
    assertEquals(0.4F, parseFloat(publisherMetricsForTopic2.get(QPS.getName())), deltaForQPS);
    assertEquals(0F, parseFloat(publisherMetricsForTopic2.get(ERROR_RATE.getName())), noDelta);

    // assert information for subscriber of topic 1
    Map<String, String> subscriberMetricsForTopic1 =
        convertToMetricMap(statisticsResponse.getSubscriberByTopicMap().get(TEST_TOPIC_1));

    assertEquals(3, Integer.parseInt(subscriberMetricsForTopic1.get(MESSAGE_COUNT.getName())));
    assertEquals(
        125.0F, parseFloat(subscriberMetricsForTopic1.get(AVG_LATENCY.getName())), deltaForLatency);
    assertEquals(
        3000.0F,
        parseFloat(subscriberMetricsForTopic1.get(THROUGHPUT.getName())),
        deltaForThroughput);
    assertEquals(0.6F, parseFloat(subscriberMetricsForTopic1.get(QPS.getName())), deltaForQPS);
    assertNull(subscriberMetricsForTopic1.get(ERROR_RATE.getName()));

    // assert information for subscriber of topic 2 (with no compute information)
    Map<String, String> subscriberMetricsForTopic2 =
        convertToMetricMap(statisticsResponse.getSubscriberByTopicMap().get(TEST_TOPIC_2));

    assertEquals(0, Integer.parseInt(subscriberMetricsForTopic2.get(MESSAGE_COUNT.getName())));
    assertEquals(0.00F, parseFloat(subscriberMetricsForTopic2.get(AVG_LATENCY.getName())), noDelta);
    assertEquals(0.00F, parseFloat(subscriberMetricsForTopic2.get(THROUGHPUT.getName())), noDelta);
    assertEquals(0.00F, parseFloat(subscriberMetricsForTopic2.get(QPS.getName())), noDelta);
    assertNull(subscriberMetricsForTopic2.get(ERROR_RATE.getName()));
  }

  @Test
  public void configuration() {

    ConfigurationResponse configurationResponse =
        blockingStub.configuration(ConfigurationRequest.newBuilder().build());

    assertEquals(Extension.YAML, configurationResponse.getExtension());
    assertFalse(configurationResponse.getContent().isEmpty());
    // Verify if file container some properties.
    assertTrue(configurationResponse.getContent().contains("kafka:"));
    assertTrue(configurationResponse.getContent().contains("server:"));
    assertTrue(configurationResponse.getContent().contains("producer:"));
    assertTrue(configurationResponse.getContent().contains("consumer:"));
  }

  private Map<String, StatisticsInformation> givenSubscriberInformation() {
    Map<String, StatisticsInformation> map = new HashMap<>();
    map.put(TEST_TOPIC_1, givenStatisticsInformation(0, 200L, 50L, 100L));
    map.put(TEST_TOPIC_2, new StatisticsInformation());
    return map;
  }

  private Map<String, StatisticsInformation> givenPublishInformation() {
    Map<String, StatisticsInformation> map = new HashMap<>();
    map.put(
        TEST_TOPIC_1,
        givenStatisticsInformation(1, 10L, 55L, 500L, 59L, 19L, 10L, 11L, 1L, 91L, 5L));
    map.put(TEST_TOPIC_2, givenStatisticsInformation(0, 90L, 90L));
    return map;
  }

  private StatisticsInformation givenStatisticsInformation(int errors, long... latencies) {
    StatisticsInformation information = new StatisticsInformation();
    for (long latency : latencies) {
      information.compute(latency, 5000);
    }
    for (int i = 0; i < errors; i++) {
      information.computeError();
    }
    return information;
  }

  private Map<String, String> convertToMetricMap(StatisticsConsolidation consolidation) {
    return consolidation
        .getMetricsList()
        .stream()
        .collect(Collectors.toMap(Metric::getName, Metric::getValue));
  }
}
