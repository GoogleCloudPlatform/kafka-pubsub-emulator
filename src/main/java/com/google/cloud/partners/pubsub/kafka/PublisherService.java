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

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.KAFKA_TOPIC;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationAlreadyExistsException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationNotFoundException;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Implementation of <a
 * href="https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#publisher"
 * target="_blank"> Cloud Pub/Sub Publisher API.</a>
 */
@Singleton
class PublisherService extends PublisherImplBase {

  private static final Logger LOGGER = Logger.getLogger(PublisherService.class.getName());
  private static final int MAX_PUBLISH_WAIT = 9; // seconds, 10s is the default publish RPC timeout

  private final ConfigurationRepository configurationRepository;
  private final List<Producer<String, ByteBuffer>> kafkaProducers = new ArrayList<>();
  private final AtomicInteger nextProducerIndex;
  private final StatisticsManager statisticsManager;

  @Inject
  PublisherService(
      ConfigurationRepository configurationRepository,
      KafkaClientFactory kafkaClientFactory,
      StatisticsManager statisticsManager) {
    this.configurationRepository = configurationRepository;
    this.statisticsManager = statisticsManager;

    for (int i = 0; i < configurationRepository.getKafka().getProducerExecutors(); i++) {
      kafkaProducers.add(kafkaClientFactory.createProducer());
    }
    LOGGER.info("Created " + kafkaProducers.size() + " KafkaProducers");
    nextProducerIndex = new AtomicInteger();
  }

  /** Shutdown hook should close all Producers. */
  public void shutdown() {
    for (Producer<String, ByteBuffer> producer : kafkaProducers) {
      producer.close();
    }
    LOGGER.info("Closed " + kafkaProducers.size() + " KafkaProducers");
  }

  @Override
  public void createTopic(Topic request, StreamObserver<Topic> responseObserver) {
    try {
      configurationRepository.createTopic(request);
      responseObserver.onNext(request);
      responseObserver.onCompleted();
    } catch (ConfigurationAlreadyExistsException e) {
      responseObserver.onError(Status.ALREADY_EXISTS.withCause(e).asException());
    }
  }

  @Override
  public void deleteTopic(DeleteTopicRequest request, StreamObserver<Empty> responseObserver) {
    try {
      configurationRepository.deleteTopic(request.getTopic());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (ConfigurationNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
    }
  }

  @Override
  public void getTopic(GetTopicRequest request, StreamObserver<Topic> responseObserver) {
    Optional<Topic> topic = configurationRepository.getTopicByName(request.getTopic());
    if (!topic.isPresent()) {
      String message = request.getTopic() + " is not a valid Topic";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      responseObserver.onNext(topic.get());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void listTopics(
      ListTopicsRequest request, StreamObserver<ListTopicsResponse> responseObserver) {
    PaginationManager<Topic> paginationManager =
        new PaginationManager<>(
            configurationRepository.getTopics(request.getProject()), Topic::getName);
    ListTopicsResponse response =
        ListTopicsResponse.newBuilder()
            .addAllTopics(paginationManager.paginate(request.getPageSize(), request.getPageToken()))
            .setNextPageToken(paginationManager.getNextToken(Topic::getName))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void listTopicSubscriptions(
      ListTopicSubscriptionsRequest request,
      StreamObserver<ListTopicSubscriptionsResponse> responseObserver) {
    PaginationManager<String> paginationManager =
        new PaginationManager<>(
            configurationRepository
                .getSubscriptionsForTopic(request.getTopic())
                .stream()
                .map(com.google.pubsub.v1.Subscription::getName)
                .collect(Collectors.toList()),
            String::toString);

    ListTopicSubscriptionsResponse response =
        ListTopicSubscriptionsResponse.newBuilder()
            .addAllSubscriptions(
                paginationManager.paginate(request.getPageSize(), request.getPageToken()))
            .setNextPageToken(paginationManager.getNextToken(String::toString))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
    Optional<Topic> topic = configurationRepository.getTopicByName(request.getTopic());
    if (!topic.isPresent()) {
      String message = request.getTopic() + " is not a valid Topic";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      publishToKafka(request, topic.get(), responseObserver);
    }
  }

  // Helper method for publish requests
  private void publishToKafka(
      PublishRequest request, Topic topic, StreamObserver<PublishResponse> responseObserver) {
    Instant start = Instant.now();
    String kafkaTopic =
        topic.getLabelsOrDefault(KAFKA_TOPIC, ProjectTopicName.parse(topic.getName()).getTopic());
    int producerIndex = nextProducerIndex.getAndUpdate((value) -> ++value % kafkaProducers.size());
    Producer<String, ByteBuffer> producer = kafkaProducers.get(producerIndex);

    CountDownLatch callbacks = new CountDownLatch(request.getMessagesCount());
    AtomicInteger failures = new AtomicInteger();
    PublishResponse.Builder builder = PublishResponse.newBuilder();
    request
        .getMessagesList()
        .forEach(
            m -> {
              ProducerRecord<String, ByteBuffer> producerRecord =
                  buildProducerRecord(kafkaTopic, m);
              long publishedAt = System.currentTimeMillis();
              producer.send(
                  producerRecord,
                  (recordMetadata, exception) -> {
                    if (recordMetadata != null) {
                      builder.addMessageIds(
                          recordMetadata.partition() + "-" + recordMetadata.offset());
                      statisticsManager.computePublish(topic.getName(), m.getData(), publishedAt);
                    } else {
                      LOGGER.severe("Unable to Publish message: " + exception.getMessage());
                      statisticsManager.computePublishError(topic.getName());
                      failures.incrementAndGet();
                    }
                    callbacks.countDown();
                  });
            });

    try {
      if (!callbacks.await(MAX_PUBLISH_WAIT, TimeUnit.SECONDS)) {
        LOGGER.warning(callbacks.getCount() + " callbacks remain after " + MAX_PUBLISH_WAIT + "s");
      }

      LOGGER.fine(
          String.format(
              "Published %d of %d messages to %s using KafkaProducer %d in %dms",
              builder.getMessageIdsCount(),
              request.getMessagesCount(),
              kafkaTopic,
              producerIndex,
              Duration.between(start, Instant.now()).toMillis()));
      if (failures.get() == 0) {
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
      } else {
        String message =
            failures.get() + " of " + request.getMessagesCount() + " Messages failed to Publish";
        LOGGER.warning(message);
        responseObserver.onError(Status.INTERNAL.withDescription(message).asException());
      }
    } catch (InterruptedException e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  private ProducerRecord<String, ByteBuffer> buildProducerRecord(
      String kafkaTopic, PubsubMessage m) {
    return new ProducerRecord<String, ByteBuffer>(
        kafkaTopic,
        null,
        null,
        m.getData().asReadOnlyByteBuffer(),
        buildHeaders(m.getAttributesMap()));
  }

  private Headers buildHeaders(Map<String, String> attributesMap) {
    if (attributesMap == null || attributesMap.isEmpty()) {
      return null;
    }
    return new RecordHeaders(
        attributesMap
            .entrySet()
            .parallelStream()
            .map(attribute -> new RecordHeader(attribute.getKey(), attribute.getValue().getBytes()))
            .collect(Collectors.toList()));
  }
}
