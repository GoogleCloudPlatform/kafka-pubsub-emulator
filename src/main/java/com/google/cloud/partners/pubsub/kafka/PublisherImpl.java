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

import static com.google.cloud.partners.pubsub.kafka.Configuration.getLastNodeInTopic;

import com.google.cloud.partners.pubsub.kafka.properties.ConsumerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.ProducerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Implementation of <a
 * href="https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#publisher"
 * target="_blank"> Cloud Pub/Sub Publisher API.</a>
 *
 * <p>Utilizes up to {@link ProducerProperties#getExecutors()} KafkaProducers to publish messages to
 * the Kafka topic indicated in a PublishRequest.
 */
class PublisherImpl extends PublisherImplBase {

  private static final Logger LOGGER = Logger.getLogger(PublisherImpl.class.getName());
  private static final int MAX_PUBLISH_WAIT = 10; // 10 seconds

  private final List<Producer<String, ByteBuffer>> kafkaProducers;

  private final Map<String, Topic> topicMap;

  private final AtomicInteger nextProducerIndex;

  private final ConsumerProperties consumerProperties;

  private final StatisticsManager statisticsManager;

  public PublisherImpl(KafkaClientFactory kafkaClientFactory, StatisticsManager statisticsManager) {
    this.statisticsManager = statisticsManager;
    ProducerProperties producerProperties =
        Configuration.getApplicationProperties().getKafkaProperties().getProducerProperties();
    this.consumerProperties =
        Configuration.getApplicationProperties().getKafkaProperties().getConsumerProperties();

    kafkaProducers = new ArrayList<>();
    for (int i = 0; i < producerProperties.getExecutors(); i++) {
      kafkaProducers.add(kafkaClientFactory.createProducer());
    }
    topicMap =
        producerProperties
            .getTopics()
            .stream()
            .collect(
                Collectors.toConcurrentMap(
                    topic -> topic, topic -> Topic.newBuilder().setName(topic).build()));
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

  // Create, delete methods are not available
  @Override
  public void createTopic(Topic request, StreamObserver<Topic> responseObserver) {
    responseObserver.onError(
        Status.UNAVAILABLE.withDescription("Topic creation is not supported").asException());
  }

  @Override
  public void deleteTopic(DeleteTopicRequest request, StreamObserver<Empty> responseObserver) {
    responseObserver.onError(
        Status.UNAVAILABLE.withDescription("Topic deletion is not supported").asException());
  }

  @Override
  public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
    Topic topic = topicMap.get(getLastNodeInTopic(request.getTopic()));
    if (topic == null) {
      String message = request.getTopic() + " is not a valid Topic";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      Instant start = Instant.now();
      int producerIndex =
          nextProducerIndex.getAndUpdate((value) -> ++value % kafkaProducers.size());
      Producer<String, ByteBuffer> producer = kafkaProducers.get(producerIndex);

      CountDownLatch callbacks = new CountDownLatch(request.getMessagesCount());
      AtomicInteger failures = new AtomicInteger();
      PublishResponse.Builder builder = PublishResponse.newBuilder();
      request
          .getMessagesList()
          .forEach(
              m -> {
                ProducerRecord<String, ByteBuffer> producerRecord = buildProducerRecord(topic, m);
                long publishedAt = System.currentTimeMillis();
                producer.send(
                    producerRecord,
                    (recordMetadata, exception) -> {
                      if (recordMetadata != null) {
                        builder.addMessageIds(
                            recordMetadata.partition() + "-" + recordMetadata.offset());
                        callbacks.countDown();
                        statisticsManager.computePublish(topic.getName(), m.getData(), publishedAt);
                      } else {
                        LOGGER.severe("Unable to Publish message: " + exception.getMessage());
                        statisticsManager.computePublishError(topic.getName());
                        failures.incrementAndGet();
                      }
                    });
              });

      try {
        if (!callbacks.await(MAX_PUBLISH_WAIT, TimeUnit.SECONDS)) {
          LOGGER.warning(
              callbacks.getCount() + " callbacks remain after " + MAX_PUBLISH_WAIT + "s");
        }

        LOGGER.fine(
            "Published "
                + builder.getMessageIdsCount()
                + " of "
                + request.getMessagesCount()
                + " messages to "
                + topic.getName()
                + " using KafkaProducer "
                + producerIndex
                + " in "
                + Duration.between(start, Instant.now()).toMillis()
                + "ms");
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
  }

  private ProducerRecord<String, ByteBuffer> buildProducerRecord(Topic topic, PubsubMessage m) {
    return new ProducerRecord<String, ByteBuffer>(
        topic.getName(),
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

  @Override
  public void getTopic(GetTopicRequest request, StreamObserver<Topic> responseObserver) {
    Topic topic = topicMap.get(getLastNodeInTopic(request.getTopic()));
    if (topic == null) {
      String message = request.getTopic() + " is not a valid Topic";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      responseObserver.onNext(topic);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void listTopics(
      ListTopicsRequest request, StreamObserver<ListTopicsResponse> responseObserver) {
    List<Topic> topics =
        topicMap
            .values()
            .stream()
            .sorted(Comparator.comparing(Topic::getName))
            .collect(Collectors.toList());

    PaginationManager<Topic> paginationManager = new PaginationManager<>(topics, Topic::getName);
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
    List<String> topics =
        consumerProperties
            .getSubscriptions()
            .stream()
            .filter(s -> s.getTopic().equals(request.getTopic()))
            .map(SubscriptionProperties::getName)
            .collect(Collectors.toList());

    PaginationManager<String> paginationManager = new PaginationManager<>(topics, String::toString);

    ListTopicSubscriptionsResponse response =
        ListTopicSubscriptionsResponse.newBuilder()
            .addAllSubscriptions(
                paginationManager.paginate(request.getPageSize(), request.getPageToken()))
            .setNextPageToken(paginationManager.getNextToken(String::toString))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
