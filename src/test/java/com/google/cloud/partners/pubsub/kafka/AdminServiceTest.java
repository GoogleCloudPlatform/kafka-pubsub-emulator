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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc.AdminBlockingStub;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationRequest;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse.Extension;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsRequest;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsResponse;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import com.google.cloud.partners.pubsub.kafka.config.FakePubSubRepository;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import io.grpc.testing.GrpcServerRule;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AdminServiceTest {

  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

  private AdminBlockingStub blockingStub;
  private ConfigurationManager configurationManager =
      new ConfigurationManager(TestHelpers.SERVER_CONFIG, new FakePubSubRepository());
  @Mock private Clock mockClock;
  @Mock private StatisticsManager statisticsManager;

  @Before
  public void setUp() {
    Instant now = Instant.ofEpochSecond(1546300800);
    when(mockClock.instant()).thenReturn(now, now.plusSeconds(60));

    AdminService admin = new AdminService(configurationManager, mockClock, statisticsManager);
    grpcServerRule.getServiceRegistry().addService(admin);
    blockingStub = AdminGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void statistics() throws ParseException {
    when(statisticsManager.getPublishInformationByTopic())
        .thenReturn(
            ImmutableMap.of(
                TestHelpers.PROJECT1_TOPIC1,
                givenStatisticsInformation(1, 10L, 55L, 500L, 59L, 19L, 10L, 11L, 1L, 91L, 5L),
                TestHelpers.PROJECT1_TOPIC2,
                givenStatisticsInformation(0, 90L, 90L),
                TestHelpers.PROJECT2_TOPIC1,
                givenStatisticsInformation(1, 10L, 55L, 500L, 59L, 19L, 10L, 11L, 1L, 91L, 5L),
                TestHelpers.PROJECT2_TOPIC2,
                givenStatisticsInformation(0, 90L, 90L)));
    when(statisticsManager.getSubscriberInformationByTopic())
        .thenReturn(
            new ImmutableMap.Builder<String, StatisticsInformation>()
                .put(TestHelpers.PROJECT1_TOPIC1, givenStatisticsInformation(0, 200L, 50L, 100L))
                .put(TestHelpers.PROJECT1_TOPIC2, new StatisticsInformation())
                .put(TestHelpers.PROJECT2_TOPIC1, givenStatisticsInformation(0, 200L, 50L, 100L))
                .put(TestHelpers.PROJECT2_TOPIC2, new StatisticsInformation())
                .build());

    StatisticsResponse statisticsResponse =
        blockingStub.statistics(StatisticsRequest.newBuilder().build());
    assertThat(statisticsResponse, Matchers.equalTo(getExpectedResponse()));
  }

  @Test
  public void configuration() throws IOException {
    ConfigurationResponse configurationResponse =
        blockingStub.configuration(ConfigurationRequest.newBuilder().build());

    assertThat(configurationResponse.getExtension(), Matchers.equalTo(Extension.JSON));
    assertThat(configurationResponse.getContent(), Matchers.equalTo(TestHelpers.PUBSUB_JSON));
  }

  private StatisticsInformation givenStatisticsInformation(int errors, long... latencies) {
    StatisticsInformation information = new StatisticsInformation();
    for (long latency : latencies) {
      information.compute(latency, 1000);
    }
    for (int i = 0; i < errors; i++) {
      information.computeError();
    }
    return information;
  }

  private StatisticsResponse getExpectedResponse() throws ParseException {
    StatisticsResponse.Builder builder = StatisticsResponse.newBuilder();
    TextFormat.getParser()
        .merge(
            "publisherExecutors: 4\n"
                + "subscriberExecutors: 4\n"
                + "publisherByTopic {\n"
                + "  key: \"projects/project-2/topics/topic-1\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"10\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"166.67\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"76.10\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.17\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"error_rate\"\n"
                + "      description: \"Percentage of requests resulting in errors.\"\n"
                + "      value: \"9.09\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "publisherByTopic {\n"
                + "  key: \"projects/project-2/topics/topic-2\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"2\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"33.33\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"90.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.03\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"error_rate\"\n"
                + "      description: \"Percentage of requests resulting in errors.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "publisherByTopic {\n"
                + "  key: \"projects/project-1/topics/topic-1\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"10\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"166.67\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"76.10\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.17\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"error_rate\"\n"
                + "      description: \"Percentage of requests resulting in errors.\"\n"
                + "      value: \"9.09\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "publisherByTopic {\n"
                + "  key: \"projects/project-1/topics/topic-2\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"2\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"33.33\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"90.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.03\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"error_rate\"\n"
                + "      description: \"Percentage of requests resulting in errors.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "subscriberByTopic {\n"
                + "  key: \"projects/project-2/topics/topic-1\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"3\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"50.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"116.67\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.05\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "subscriberByTopic {\n"
                + "  key: \"projects/project-2/topics/topic-2\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"0\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "subscriberByTopic {\n"
                + "  key: \"projects/project-1/topics/topic-1\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"3\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"50.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"116.67\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.05\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "subscriberByTopic {\n"
                + "  key: \"projects/project-1/topics/topic-2\"\n"
                + "  value {\n"
                + "    metrics {\n"
                + "      name: \"message_count\"\n"
                + "      description: \"Count of messages processed by emulator.\"\n"
                + "      value: \"0\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"throughput\"\n"
                + "      description: \"Throughput in bytes per second\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"average_latency\"\n"
                + "      description: \"Average latency per request in milliseconds.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "    metrics {\n"
                + "      name: \"qps\"\n"
                + "      description: \"QPS.\"\n"
                + "      value: \"0.00\"\n"
                + "    }\n"
                + "  }\n"
                + "}",
            builder);
    return builder.build();
  }
}
