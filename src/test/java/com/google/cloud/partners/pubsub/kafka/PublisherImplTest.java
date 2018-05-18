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

import static com.google.cloud.partners.pubsub.kafka.TestHelpers.PROJECT;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC2;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC_NOT_EXISTS;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.generatePubsubMessages;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.generatePubsubMessagesWithHeader;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.resetRequestBindConfiguration;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.setupRequestBindConfiguration;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.useTestApplicationConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.Topic;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;

@RunWith(MockitoJUnitRunner.class)
public class PublisherImplTest {

  private static final String MESSAGE_CONTENT_REGEX = "message-[0-9]";

  private static final ScheduledExecutorService PUBLISH_EXECUTOR =
      Executors.newSingleThreadScheduledExecutor();
  private static final String PROJECT_TOPIC_FORMAT = "projects/%s/topics/%s";

  @Rule
  public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

  private PublisherGrpc.PublisherBlockingStub blockingStub;
  private MockKafkaClientFactoryImpl kafkaClientFactory;
  private PublisherImpl publisher;

  @Mock
  private StatisticsManager statisticsManager;

  @BeforeClass
  public static void setUpBeforeClass() {
    useTestApplicationConfig(1, 1);
  }

  @Before
  public void setUp() {
    setupRequestBindConfiguration();
    kafkaClientFactory = new MockKafkaClientFactoryImpl();
    publisher = new PublisherImpl(kafkaClientFactory, statisticsManager);
    grpcServerRule.getServiceRegistry().addService(publisher);
    blockingStub = PublisherGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @After
  public void tearDown() {
    resetRequestBindConfiguration();
  }

  @Test
  public void shutdown() {
    publisher.shutdown();
    kafkaClientFactory.getCreatedProducers().forEach(p -> assertTrue(p.closed()));
  }

  @Test
  public void createTopic() {
    try {
      Topic request = Topic.newBuilder().setName(TOPIC1).build();
      blockingStub.createTopic(request);
      fail("Topic creation should be unavailable");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.UNAVAILABLE.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void deleteTopic() {
    try {
      DeleteTopicRequest request =
          DeleteTopicRequest.newBuilder().setTopic(TOPIC1).build();
      blockingStub.deleteTopic(request);
      fail("Topic deletion should be unavailable");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.UNAVAILABLE.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void getTopicExists() {
    GetTopicRequest request = GetTopicRequest.newBuilder().setTopic(TOPIC1).build();
    Topic response = blockingStub.getTopic(request);
    assertEquals(TOPIC1, response.getName());
  }

  @Test
  public void getTopicDoesNotExist() {
    try {
      GetTopicRequest request =
          GetTopicRequest.newBuilder().setTopic(TOPIC_NOT_EXISTS).build();
      blockingStub.getTopic(request);
      fail("Topic should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void listTopics() {
    ListTopicsRequest request = ListTopicsRequest.getDefaultInstance();
    ListTopicsResponse response = blockingStub.listTopics(request);

    assertEquals(2, response.getTopicsCount());
    assertEquals(TOPIC1, response.getTopics(0).getName());
    assertEquals(TOPIC2, response.getTopics(1).getName());
  }

  @Test
  public void listTopicsWithPagination() {
    ListTopicsRequest request = ListTopicsRequest.newBuilder().setPageSize(1).build();
    ListTopicsResponse responsePageOne = blockingStub.listTopics(request);

    assertEquals(1, responsePageOne.getTopicsCount());
    assertEquals(TOPIC1, responsePageOne.getTopics(0).getName());
    assertFalse(responsePageOne.getNextPageToken().isEmpty());

    ListTopicsResponse responsePageTwo =
        blockingStub.listTopics(
            ListTopicsRequest.newBuilder()
                .setPageSize(1)
                .setPageToken(responsePageOne.getNextPageToken())
                .build());

    assertEquals(1, responsePageTwo.getTopicsCount());
    assertEquals(TOPIC2, responsePageTwo.getTopics(0).getName());
    assertTrue(responsePageTwo.getNextPageToken().isEmpty());
  }

  @Test
  public void listTopicSubscriptions() {
    // TestHelpers.TOPIC1
    ListTopicSubscriptionsRequest request =
        ListTopicSubscriptionsRequest.newBuilder().setTopic(TOPIC1).build();
    ListTopicSubscriptionsResponse response = blockingStub.listTopicSubscriptions(request);
    assertEquals(2, response.getSubscriptionsCount());
    assertEquals("subscription-1-to-test-topic-1", response.getSubscriptions(0));
    assertEquals("subscription-2-to-test-topic-1", response.getSubscriptions(1));

    // TestHelpers.TOPIC2
    request = ListTopicSubscriptionsRequest.newBuilder().setTopic(TOPIC2).build();
    response = blockingStub.listTopicSubscriptions(request);
    assertEquals(1, response.getSubscriptionsCount());
    assertEquals("subscription-1-to-test-topic-2", response.getSubscriptions(0));

    // None
    request =
        ListTopicSubscriptionsRequest.newBuilder().setTopic(TOPIC_NOT_EXISTS).build();
    response = blockingStub.listTopicSubscriptions(request);
    assertEquals(0, response.getSubscriptionsCount());
  }

  @Test
  public void listTopicSubscriptionsWithPagination() {
    // TestHelpers.TOPIC1
    ListTopicSubscriptionsRequest request =
        ListTopicSubscriptionsRequest.newBuilder()
            .setPageSize(1)
            .setTopic(TOPIC1)
            .build();

    ListTopicSubscriptionsResponse responsePageOne = blockingStub.listTopicSubscriptions(request);
    assertEquals(1, responsePageOne.getSubscriptionsCount());
    assertEquals("subscription-1-to-test-topic-1", responsePageOne.getSubscriptions(0));
    assertFalse(responsePageOne.getNextPageToken().isEmpty());

    ListTopicSubscriptionsRequest secondRequest =
        ListTopicSubscriptionsRequest.newBuilder()
            .setPageSize(1)
            .setPageToken(responsePageOne.getNextPageToken())
            .setTopic(TOPIC1)
            .build();

    ListTopicSubscriptionsResponse responsePageTwo =
        blockingStub.listTopicSubscriptions(secondRequest);

    assertEquals(1, responsePageTwo.getSubscriptionsCount());
    assertEquals("subscription-2-to-test-topic-1", responsePageTwo.getSubscriptions(0));
    assertTrue(responsePageTwo.getNextPageToken().isEmpty());
  }

  @Test
  public void publishDoesNotExist() {
    try {
      PublishRequest request =
          PublishRequest.newBuilder()
              .setTopic(TOPIC_NOT_EXISTS)
              .addAllMessages(generatePubsubMessages(5))
              .build();
      blockingStub.publish(request);
      fail("Topic should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  /*
  Publish tests need to manipulate the MockProducer in a separate thread from the blocking publish
  request so we'll use the PUBLISH_EXECUTOR to submit Runnables that implement the desired
  producer behaviors.
   */
  @Test
  public void publishSingleMessage() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(TOPIC1)
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
            producer.completeNext();
          }
        });

    PublishResponse response = blockingStub.publish(request);
    assertEquals(5, response.getMessageIdsCount());
    assertEquals("0-0", response.getMessageIds(0));
    assertEquals("0-1", response.getMessageIds(1));
    assertEquals("0-2", response.getMessageIds(2));
    assertEquals("0-3", response.getMessageIds(3));
    assertEquals("0-4", response.getMessageIds(4));

    verify(statisticsManager, times(5))
        .computePublish(
            eq(TOPIC1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publishSingleMessageWithRequestBind() {
    int messages = 1;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(String.format(PROJECT_TOPIC_FORMAT, PROJECT, TOPIC2))
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
            producer.completeNext();
          }
        });

    PublishResponse response = blockingStub.publish(request);
    assertEquals(1, response.getMessageIdsCount());
    assertEquals("0-0", response.getMessageIds(0));

    verify(statisticsManager, times(1))
        .computePublish(
            eq(TOPIC2),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publishSingleMessageFailsWithRequestBind() {
    int messages = 1;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(String.format(PROJECT_TOPIC_FORMAT, "not-mapped", TOPIC2))
            .addAllMessages(generatePubsubMessages(messages))
            .build();

    try {
      blockingStub.publish(request);
      fail("Publish operation should fail");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
      assertTrue(e.getMessage().contains("is not a valid Topic"));
      verifyZeroInteractions(statisticsManager);
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void publishSingleMessageWithAttributesMap() {
    int messages = 3;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(TOPIC1)
            .addAllMessages(generatePubsubMessagesWithHeader(messages))
            .build();

    final MockProducer<String, ByteBuffer> producer =
        kafkaClientFactory.getCreatedProducers().get(0);

    PUBLISH_EXECUTOR.submit(
        () -> {
          while (producer.history().size() < messages) {
            Thread.yield();
          }
          for (int i = 0; i < messages; i++) {
            producer.completeNext();
          }
        });

    PublishResponse response = blockingStub.publish(request);
    assertEquals(3, response.getMessageIdsCount());
    assertEquals("0-0", response.getMessageIds(0));
    assertEquals("0-1", response.getMessageIds(1));
    assertEquals("0-2", response.getMessageIds(2));

    List<ProducerRecord<String, ByteBuffer>> history = producer.history();

    String expectedHeaderKey = "some-key";
    String expectedHeaderValue = "some-value";

    assertEquals(3, history.size());
    assertEquals(
        expectedHeaderValue,
        new String(history.get(0).headers().lastHeader(expectedHeaderKey).value()));
    assertEquals(
        expectedHeaderValue,
        new String(history.get(1).headers().lastHeader(expectedHeaderKey).value()));
    assertEquals(
        expectedHeaderValue,
        new String(history.get(2).headers().lastHeader(expectedHeaderKey).value()));

    verify(statisticsManager, times(3))
        .computePublish(
            eq(TOPIC1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }

  @Test
  public void publishProducerFails() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(TOPIC1)
            .addAllMessages(generatePubsubMessages(messages))
            .build();

    PUBLISH_EXECUTOR.submit(
        () -> {
          MockProducer<String, ByteBuffer> producer =
              kafkaClientFactory.getCreatedProducers().get(0);
          while (!producer.errorNext(new RuntimeException("Send Operation Failed"))) {
            Thread.yield();
          }
        });

    try {
      blockingStub.publish(request);
      fail("Publish operation should fail");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.INTERNAL.getCode(), e.getStatus().getCode());

      verify(statisticsManager).computePublishError(eq(TOPIC1));
      verify(statisticsManager, never())
          .computePublish(anyString(), any(ByteString.class), anyLong());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void publishProducerTimesOut() {
    int messages = 5;
    PublishRequest request =
        PublishRequest.newBuilder()
            .setTopic(TOPIC1)
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
    assertEquals(4, response.getMessageIdsCount());
    assertEquals("0-0", response.getMessageIds(0));
    assertEquals("0-1", response.getMessageIds(1));
    assertEquals("0-2", response.getMessageIds(2));
    assertEquals("0-3", response.getMessageIds(3));

    verify(statisticsManager, times(4))
        .computePublish(
            eq(TOPIC1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            anyLong());
    verify(statisticsManager, never()).computePublishError(anyString());
  }
}
