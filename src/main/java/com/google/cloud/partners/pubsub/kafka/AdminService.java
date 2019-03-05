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
import static java.lang.String.format;

import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc.AdminImplBase;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationRequest;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse;
import com.google.cloud.partners.pubsub.kafka.common.ConfigurationResponse.Extension;
import com.google.cloud.partners.pubsub.kafka.common.Metric;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsConsolidation;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsRequest;
import com.google.cloud.partners.pubsub.kafka.common.StatisticsResponse;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import com.google.cloud.partners.pubsub.kafka.enums.MetricProperty;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.Topic;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Singleton;

/** Administrative functions service. */
@Singleton
class AdminService extends AdminImplBase {

  private static final String DECIMAL_FORMAT = "%19.2f";
  private static final String FORMAT = "%d";

  private final ConfigurationManager configurationManager;
  private final StatisticsManager statisticsManager;
  private final Clock clock;
  private final Instant startedAt;

  @Inject
  AdminService(
      ConfigurationManager configurationManager, Clock clock, StatisticsManager statisticsManager) {
    this.configurationManager = configurationManager;
    this.clock = clock;
    this.startedAt = clock.instant();
    this.statisticsManager = statisticsManager;
  }

  // TODO: Replace ConfigurationResponse with the actual proto
  @Override
  public void configuration(
      ConfigurationRequest request, StreamObserver<ConfigurationResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ConfigurationResponse.newBuilder()
              .setContent(JsonFormat.printer().print(configurationManager.getPubSub()))
              .setExtension(Extension.JSON)
              .build());
      responseObserver.onCompleted();
    } catch (InvalidProtocolBufferException e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void statistics(
      StatisticsRequest request, StreamObserver<StatisticsResponse> responseObserver) {
    long durationSeconds = java.time.Duration.between(startedAt, clock.instant()).getSeconds();
    Map<String, StatisticsInformation> publishInformationByTopic =
        statisticsManager.getPublishInformationByTopic();
    Map<String, StatisticsInformation> subscriberInformationByTopic =
        statisticsManager.getSubscriberInformationByTopic();

    Map<String, StatisticsConsolidation> publishResultByTopic =
        processResult(
            topic ->
                StatisticsConsolidation.newBuilder()
                    .addAllMetrics(
                        calculatePublisherInformation(
                            durationSeconds, publishInformationByTopic.get(topic)))
                    .build());
    Map<String, StatisticsConsolidation> subscriberResultByTopic =
        processResult(
            topic ->
                StatisticsConsolidation.newBuilder()
                    .addAllMetrics(
                        calculateInformation(
                            durationSeconds, subscriberInformationByTopic.get(topic)))
                    .build());

    StatisticsResponse response =
        StatisticsResponse.newBuilder()
            .setPublisherExecutors(
                configurationManager.getServer().getKafka().getProducerExecutors())
            .setSubscriberExecutors(
                configurationManager.getServer().getKafka().getConsumersPerSubscription())
            .putAllPublisherByTopic(publishResultByTopic)
            .putAllSubscriberByTopic(subscriberResultByTopic)
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private List<Metric> calculateInformation(
      long durationSeconds, StatisticsInformation information) {
    Metric count = buildMetric(MESSAGE_COUNT, information.getCount().intValue(), FORMAT);
    Metric throughput =
        buildMetric(THROUGHPUT, information.getThroughput(durationSeconds), DECIMAL_FORMAT);
    Metric averageLatency =
        buildMetric(AVG_LATENCY, information.getAverageLatency(), DECIMAL_FORMAT);
    Metric qps = buildMetric(QPS, information.getQPS(durationSeconds), DECIMAL_FORMAT);
    return Lists.newArrayList(count, throughput, averageLatency, qps);
  }

  private List<Metric> calculatePublisherInformation(
      long durationSeconds, StatisticsInformation information) {
    List<Metric> metrics = calculateInformation(durationSeconds, information);
    metrics.add(buildMetric(ERROR_RATE, information.getErrorRating(), DECIMAL_FORMAT));
    return metrics;
  }

  private Map<String, StatisticsConsolidation> processResult(
      Function<String, StatisticsConsolidation> function) {
    return configurationManager
        .getProjects()
        .stream()
        .flatMap(project -> configurationManager.getTopics(project).stream())
        .map(Topic::getName)
        .collect(Collectors.toMap(Function.identity(), function));
  }

  private Metric buildMetric(MetricProperty property, Object value, String format) {
    return Metric.newBuilder()
        .setDescription(property.getDescription())
        .setName(property.getName())
        .setValue(format(format, value).trim())
        .build();
  }
}
