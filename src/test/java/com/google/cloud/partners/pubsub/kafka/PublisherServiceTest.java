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

import static com.google.cloud.partners.pubsub.kafka.TestHelpers.generatePubsubMessages;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.generatePubsubMessagesWithHeader;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import com.google.cloud.partners.pubsub.kafka.config.FakePubSubRepository;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.Topic;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PublisherServiceTest {

  private static final String MESSAGE_CONTENT_REGEX = "message-[0-9]";

  private static final ScheduledExecutorService PUBLISH_EXECUTOR =
      Executors.newSingleThreadScheduledExecutor();
  private static final String KAFKA_TOPIC = "kafka-topic";

  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private PublisherGrpc.PublisherBlockingStub blockingStub;
  private MockKafkaClientFactory kafkaClientFactory;
  private PublisherService publisher;

  @Mock private StatisticsManager statisticsManager;

  @Before
  public void setUp() {
    kafkaClientFactory = new MockKafkaClientFactory();
    publisher =
        new PublisherService(
            new ConfigurationManager(TestHelpers.SERVER_CONFIG, new FakePubSubRepository()),
            kafkaClientFactory,
            statisticsManager);
    grpcServerRule.getServiceRegistry().addService(publisher);
    blockingStub = PublisherGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void shutdown() {
    publisher.shutdown();
    kafkaClientFactory.getCreatedProducers().forEach(p -> assertTrue(p.closed()));
  }

  @Test
  public void createTopic() {
    Topic request = Topic.newBuilder().setName("projects/project-1/topics/new-topic").build();
    assertThat(blockingStub.createTopic(request), Matchers.equalTo(request));
  }

  @Test
  public void createTopic_topicExists() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.ALREADY_EXISTS.getCode().toString());

    Topic request = Topic.newBuilder().setName("projects/project-1/topics/topic-1").build();

    blockingStub.createTopic(request);
  }

  @Test
  public void deleteTopic() {
    String topicName = "projects/project-1/topics/topic-1";
    DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topicName).build();

    blockingStub.deleteTopic(request);

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    blockingStub.getTopic(GetTopicRequest.newBuilder().setTopic(topicName).build());
  }

  @Test
  public void deleteTopic_topicDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    String topicName = "projects/project-1/topics/unknown-topic";
    DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topicName).build();

    blockingStub.deleteTopic(request);
  }

  @Test
  public void getTopic() {
    String topicName = "projects/project-1/topics/topic-1";

    GetTopicRequest request = GetTopicRequest.newBuilder().setTopic(topicName).build();
    Topic response = blockingStub.getTopic(request);
    assertThat(response.getName(), Matchers.equalTo(topicName));
  }

  @Test
  public void getTopic_topicDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    blockingStub.getTopic(
        GetTopicRequest.newBuilder()
            .setTopic(ProjectTopicName.of("project-1", "unknown-topic").toString())
            .build());
  }

  @Test
  public void listTopics() {
    ListTopicsRequest request =
        ListTopicsRequest.newBuilder().setProject("projects/project-1").build();
    ListTopicsResponse response = blockingStub.listTopics(request);

    assertThat(
        response.getTopicsList(),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .build(),
            com.google.pubsub.v1.Topic.newBuilder()
                .setName(TestHelpers.PROJECT1_TOPIC2)
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
  }

  @Test
  public void listTopics_withPagination() {
    ListTopicsRequest request =
        ListTopicsRequest.newBuilder().setProject("projects/project-1").setPageSize(1).build();
    ListTopicsResponse response = blockingStub.listTopics(request);

    assertThat(response.getTopicsList(), Matchers.hasSize(1));
    assertThat(
        response.getTopicsList(),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .build()));
    assertThat(response.getNextPageToken(), Matchers.not(Matchers.isEmptyOrNullString()));

    request = request.toBuilder().setPageToken(response.getNextPageToken()).setPageSize(0).build();
    response = blockingStub.listTopics(request);
    assertThat(response.getTopicsList(), Matchers.hasSize(1));
    assertThat(
        response.getTopicsList(),
        Matchers.contains(
            com.google.pubsub.v1.Topic.newBuilder()
                .setName("projects/project-1/topics/topic-2")
                .putLabels(KAFKA_TOPIC, "kafka-topic-2")
                .build()));
    assertThat(response.getNextPageToken(), Matchers.isEmptyOrNullString());
  }

  @Test
  public void listTopicSubscriptions() {
    ListTopicSubscriptionsRequest request =
        ListTopicSubscriptionsRequest.newBuilder()
            .setTopic("projects/project-2/topics/topic-1")
            .build();
    ListTopicSubscriptionsResponse response = blockingStub.listTopicSubscriptions(request);
    assertThat(response.getSubscriptionsList(), Matchers.hasSize(2));
    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            "projects/project-2/subscriptions/subscription-1",
            "projects/project-2/subscriptions/subscription-2"));
    assertThat(response.getNextPageToken(), Matchers.isEmptyOrNullString());
  }

  @Test
  public void listTopicSubscriptions_withPagination() {
    ListTopicSubscriptionsRequest request =
        ListTopicSubscriptionsRequest.newBuilder()
            .setTopic(TestHelpers.PROJECT1_TOPIC2)
            .setPageSize(1)
            .build();
    ListTopicSubscriptionsResponse response = blockingStub.listTopicSubscriptions(request);
    assertThat(response.getSubscriptionsList(), Matchers.hasSize(1));
    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains("projects/project-1/subscriptions/subscription-3"));
    assertThat(response.getNextPageToken(), Matchers.not(Matchers.isEmptyOrNullString()));

    request = request.toBuilder().setPageToken(response.getNextPageToken()).setPageSize(0).build();
    response = blockingStub.listTopicSubscriptions(request);
    assertThat(response.getSubscriptionsList(), Matchers.hasSize(1));
    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains("projects/project-1/subscriptions/subscription-4"));
    assertThat(response.getNextPageToken(), Matchers.isEmptyOrNullString());
  }

  /**
   * Publish tests need to manipulate the MockProducer in a separate thread from the blocking
   * publish request so we'll use the PUBLISH_EXECUTOR to submit Runnables that implement the
   * desired producer behaviors.
   */
  @Test
  public void publish() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/topic-1")
            .addAllMessages(generatePubsubMessages(messages))
            .build();

    MockProducer<String, ByteBuffer> mockProducer = startPublishExecutor(messages);

    PublishResponse response = blockingStub.publish(request);
    List<String> topics = new ArrayList<>();
    List<String> data = new ArrayList<>();
    for (ProducerRecord<String, ByteBuffer> producerRecord : mockProducer.history()) {
      topics.add(producerRecord.topic());
      data.add(UTF_8.decode(producerRecord.value()).toString());
    }

    assertThat(response.getMessageIdsList(), Matchers.contains("0-0", "0-1", "0-2", "0-3", "0-4"));
    assertThat(
        topics,
        Matchers.contains(
            "kafka-topic-1", "kafka-topic-1", "kafka-topic-1", "kafka-topic-1", "kafka-topic-1"));
    assertThat(
        data, Matchers.contains("message-0", "message-1", "message-2", "message-3", "message-4"));

    verify(statisticsManager, times(5))
        .computePublish(
            eq("projects/project-1/topics/topic-1"),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publish_implicitKafkaTopic() {
    blockingStub.createTopic(
        Topic.newBuilder().setName("projects/project-1/topics/implicit-kafka-topic").build());

    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/implicit-kafka-topic")
            .addAllMessages(generatePubsubMessages(messages))
            .build();

    MockProducer<String, ByteBuffer> mockProducer = startPublishExecutor(messages);

    PublishResponse response = blockingStub.publish(request);
    List<String> topics = new ArrayList<>();
    List<String> data = new ArrayList<>();
    for (ProducerRecord<String, ByteBuffer> producerRecord : mockProducer.history()) {
      topics.add(producerRecord.topic());
      data.add(UTF_8.decode(producerRecord.value()).toString());
    }

    assertThat(response.getMessageIdsList(), Matchers.contains("0-0", "0-1", "0-2", "0-3", "0-4"));
    assertThat(
        topics,
        Matchers.contains(
            "implicit-kafka-topic",
            "implicit-kafka-topic",
            "implicit-kafka-topic",
            "implicit-kafka-topic",
            "implicit-kafka-topic"));
    assertThat(
        data, Matchers.contains("message-0", "message-1", "message-2", "message-3", "message-4"));

    verify(statisticsManager, times(5))
        .computePublish(
            eq("projects/project-1/topics/implicit-kafka-topic"),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publish_topicDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/unknown")
            .addAllMessages(generatePubsubMessages(1))
            .build();
    blockingStub.publish(request);
  }

  @Test
  public void publish_withAttributes() {
    int messages = 3;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/topic-2")
            .addAllMessages(generatePubsubMessagesWithHeader(messages))
            .build();

    MockProducer<String, ByteBuffer> producer = startPublishExecutor(messages);

    PublishResponse response = blockingStub.publish(request);
    assertThat(response.getMessageIdsList(), Matchers.contains("0-0", "0-1", "0-2"));

    List<Headers> headers =
        producer.history().stream().map(ProducerRecord::headers).collect(Collectors.toList());
    assertThat(
        headers,
        Matchers.contains(
            new RecordHeaders(
                Collections.singletonList(
                    new RecordHeader("some-key", "some-value".getBytes(UTF_8)))),
            new RecordHeaders(
                Collections.singletonList(
                    new RecordHeader("some-key", "some-value".getBytes(UTF_8)))),
            new RecordHeaders(
                Collections.singletonList(
                    new RecordHeader("some-key", "some-value".getBytes(UTF_8))))));

    verify(statisticsManager, times(3))
        .computePublish(
            eq("projects/project-1/topics/topic-2"),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publish_producerFails() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/topic-1")
            .addAllMessages(generatePubsubMessages(messages))
            .build();

    PUBLISH_EXECUTOR.submit(
        () -> {
          MockProducer<String, ByteBuffer> producer =
              kafkaClientFactory.getCreatedProducers().get(0);
          while (producer.history().size() < messages) {
            Thread.yield();
          }
          for (int i = 0; i < messages; i++) {
            producer.errorNext(new RuntimeException("Send Operation Failed"));
          }
        });

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.INTERNAL.getCode().toString());

    blockingStub.publish(request);
    verify(statisticsManager).computePublishError(eq("projects/project-1/topics/topic-1"));
    verify(statisticsManager, never())
        .computePublish(anyString(), any(ByteString.class), anyLong());
  }

  @Test
  public void publish_producerTimesOut() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic("projects/project-1/topics/topic-1")
            .addAllMessages(generatePubsubMessages(messages))
            .build();
    PUBLISH_EXECUTOR.submit(
        () -> {
          MockProducer<String, ByteBuffer> producer =
              kafkaClientFactory.getCreatedProducers().get(0);
          while (producer.history().size() < messages) {
            Thread.yield();
          }
          for (int i = 0; i < messages - 1; i++) {
            producer.completeNext();
          }
        });

    PublishResponse response = blockingStub.publish(request);
    assertThat(response.getMessageIdsList(), Matchers.contains("0-0", "0-1", "0-2", "0-3"));

    verify(statisticsManager, times(4))
        .computePublish(
            eq("projects/project-1/topics/topic-1"),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  // Submits a Runnable to the PUBLISH_EXECUTOR to simulate the successful Kafka publish operation
  private MockProducer<String, ByteBuffer> startPublishExecutor(int numberOfMessages) {
    MockProducer<String, ByteBuffer> producer = kafkaClientFactory.getCreatedProducers().get(0);
    PUBLISH_EXECUTOR.submit(
        () -> {
          while (producer.history().size() < numberOfMessages) {
            Thread.yield();
          }
          for (int i = 0; i < numberOfMessages; i++) {
            producer.completeNext();
          }
        });
    return producer;
  }
}
