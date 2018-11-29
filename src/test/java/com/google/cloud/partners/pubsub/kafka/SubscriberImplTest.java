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

import static com.google.cloud.partners.pubsub.kafka.TestHelpers.NEW_SUBSCRIPTION1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.NEW_SUBSCRIPTION2;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.NEW_SUBSCRIPTION2_FORMATTED;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.PROJECT;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION2;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION3;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION_NOT_EXISTS;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION_TO_DELETE1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.SUBSCRIPTION_TO_DELETE2_FORMATTED;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC2;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC2_FORMATTED;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC_TO_DELETE1;
import static com.google.cloud.partners.pubsub.kafka.TestHelpers.TOPIC_TO_DELETE2;
import static java.lang.String.format;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberStub;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberImplTest {

  public static final String PROJECT_SUBSCRIPTION_FORMAT = "projects/%s/subscriptions/%s";
  private static final String MESSAGE_CONTENT_REGEX = "message-[0-9]{4}";
  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule();
  private SubscriberGrpc.SubscriberBlockingStub blockingStub;
  private MockKafkaClientFactoryImpl kafkaClientFactory;
  private SpyingSubscriptionManageFactoryImpl subscriptionManageFactory;
  private SubscriberImpl subscriber;
  @Mock private StatisticsManager statisticsManager;

  @BeforeClass
  public static void setUpBeforeClass() {

    TestHelpers.useTestApplicationConfig(1, 1);
  }

  @Before
  public void setUp() {
    TestHelpers.setupRequestBindConfiguration();
    kafkaClientFactory = new MockKafkaClientFactoryImpl();
    kafkaClientFactory.configureConsumersForSubscription(TOPIC1, SUBSCRIPTION1, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(TOPIC1, SUBSCRIPTION2, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(TOPIC2, SUBSCRIPTION3, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(TOPIC1, NEW_SUBSCRIPTION1, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(TOPIC2, NEW_SUBSCRIPTION2, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(
        TOPIC_TO_DELETE1, SUBSCRIPTION_TO_DELETE1, 3, 0L, 0L);
    kafkaClientFactory.configureConsumersForSubscription(
        TOPIC_TO_DELETE2, SUBSCRIPTION_TO_DELETE2_FORMATTED, 3, 0L, 0L);

    subscriptionManageFactory = new SpyingSubscriptionManageFactoryImpl();
    subscriber =
        new SubscriberImpl(kafkaClientFactory, subscriptionManageFactory, statisticsManager);
    grpcServerRule.getServiceRegistry().addService(subscriber);
    blockingStub = SubscriberGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @After
  public void tearDown() {
    List<String> toRemove = Lists.newArrayList(NEW_SUBSCRIPTION1, NEW_SUBSCRIPTION2);
    List<SubscriptionProperties> l =
        Configuration.getApplicationProperties()
            .getKafkaProperties()
            .getConsumerProperties()
            .getSubscriptions();
    l.removeIf(s -> toRemove.contains(s.getName()));

    TestHelpers.resetRequestBindConfiguration();
  }

  @Test
  public void shutdown() {
    subscriber.shutdown();
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION1)
        .forEach(c -> assertTrue(c.closed()));
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION2)
        .forEach(c -> assertTrue(c.closed()));
    kafkaClientFactory
        .getConsumersForSubscription(SUBSCRIPTION3)
        .forEach(c -> assertTrue(c.closed()));
  }

  @Test
  public void createSubscriptionWhenTopicNotExists() {
    try {
      Subscription request =
          Subscription.newBuilder()
              .setName(SUBSCRIPTION1)
              .setTopic("not-exists")
              .setAckDeadlineSeconds(10)
              .build();
      blockingStub.createSubscription(request);
      fail("Subscription creation should be unavailable");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void createSubscriptionWhenSubscriptionAlreadyCreated() {
    try {
      Subscription request =
          Subscription.newBuilder()
              .setName(SUBSCRIPTION1)
              .setTopic(TOPIC1)
              .setAckDeadlineSeconds(10)
              .build();
      blockingStub.createSubscription(request);
      fail("Subscription creation should be unavailable");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.ALREADY_EXISTS.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void createSubscriptionFormatted() {
    try {
      int ack = 10;

      Subscription request =
          Subscription.newBuilder()
              .setName(NEW_SUBSCRIPTION2_FORMATTED)
              .setTopic(TOPIC2_FORMATTED)
              .setAckDeadlineSeconds(ack)
              .build();
      blockingStub.createSubscription(request);

      SubscriptionProperties subscriptionProperties =
          Configuration.getApplicationProperties()
              .getKafkaProperties()
              .getConsumerProperties()
              .getSubscriptions()
              .stream()
              .filter(s -> s.getName().equals(NEW_SUBSCRIPTION2))
              .findFirst()
              .orElseThrow(() -> new Exception("Fail get property information."));

      assertEquals(TOPIC2, subscriptionProperties.getTopic());
      assertEquals(ack, subscriptionProperties.getAckDeadlineSeconds());
      verify(statisticsManager).addSubscriberInformation(subscriptionProperties);
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void createSubscription() {
    try {
      int ack = 10;

      Subscription request =
          Subscription.newBuilder()
              .setName(NEW_SUBSCRIPTION1)
              .setTopic(TOPIC1)
              .setAckDeadlineSeconds(ack)
              .build();
      blockingStub.createSubscription(request);

      SubscriptionProperties subscriptionProperties =
          Configuration.getApplicationProperties()
              .getKafkaProperties()
              .getConsumerProperties()
              .getSubscriptions()
              .stream()
              .filter(s -> s.getName().equals(NEW_SUBSCRIPTION1))
              .findFirst()
              .orElseThrow(() -> new Exception("Fail get property information."));

      assertEquals(TOPIC1, subscriptionProperties.getTopic());
      assertEquals(ack, subscriptionProperties.getAckDeadlineSeconds());
      verify(statisticsManager).addSubscriberInformation(subscriptionProperties);
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void deleteSubscription() {
    try {
      SubscriptionProperties subscriptionToDelete = new SubscriptionProperties();
      subscriptionToDelete.setTopic(TOPIC_TO_DELETE1);
      subscriptionToDelete.setName(SUBSCRIPTION_TO_DELETE1);
      subscriptionToDelete.setAckDeadlineSeconds(10);
      subscriber.createSubscription(subscriptionToDelete);

      DeleteSubscriptionRequest request =
          DeleteSubscriptionRequest.newBuilder().setSubscription(SUBSCRIPTION_TO_DELETE1).build();
      blockingStub.deleteSubscription(request);

      assertEquals(
          0,
          Configuration.getApplicationProperties()
              .getKafkaProperties()
              .getConsumerProperties()
              .getSubscriptions()
              .stream()
              .filter(
                  subscriptionProperties -> subscriptionProperties.equals(SUBSCRIPTION_TO_DELETE1))
              .count());

      verify(statisticsManager).removeSubscriberInformation(TOPIC_TO_DELETE1);
    } catch (StatusRuntimeException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void deleteSubscriptionWhenSubscriptNotFound() {
    try {
      DeleteSubscriptionRequest request =
          DeleteSubscriptionRequest.newBuilder().setSubscription("not_found").build();
      blockingStub.deleteSubscription(request);
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void deleteSubscriptionFormatted() {
    try {
      SubscriptionProperties subscriptionToDelete = new SubscriptionProperties();
      subscriptionToDelete.setTopic(TOPIC_TO_DELETE2);
      subscriptionToDelete.setName(SUBSCRIPTION_TO_DELETE2_FORMATTED);
      subscriptionToDelete.setAckDeadlineSeconds(10);
      subscriber.createSubscription(subscriptionToDelete);

      DeleteSubscriptionRequest request =
          DeleteSubscriptionRequest.newBuilder()
              .setSubscription(SUBSCRIPTION_TO_DELETE2_FORMATTED)
              .build();
      blockingStub.deleteSubscription(request);

      assertEquals(
          0,
          Configuration.getApplicationProperties()
              .getKafkaProperties()
              .getConsumerProperties()
              .getSubscriptions()
              .stream()
              .filter(
                  subscriptionProperties ->
                      subscriptionProperties.equals(SUBSCRIPTION_TO_DELETE2_FORMATTED))
              .count());

      verify(statisticsManager).removeSubscriberInformation(TOPIC_TO_DELETE2);
    } catch (StatusRuntimeException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void getSubscriptionExists() {
    GetSubscriptionRequest request =
        GetSubscriptionRequest.newBuilder().setSubscription(SUBSCRIPTION1).build();
    Subscription response = blockingStub.getSubscription(request);
    assertEquals(SUBSCRIPTION1, response.getName());
    assertEquals(TOPIC1, response.getTopic());
    assertEquals(10, response.getAckDeadlineSeconds());
  }

  @Test
  public void getSubscriptionDoesNotExist() {
    try {
      GetSubscriptionRequest request =
          GetSubscriptionRequest.newBuilder().setSubscription(SUBSCRIPTION_NOT_EXISTS).build();
      blockingStub.getSubscription(request);
      fail("Subscription should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void listSubscriptions() {
    ListSubscriptionsRequest request = ListSubscriptionsRequest.getDefaultInstance();
    ListSubscriptionsResponse response = blockingStub.listSubscriptions(request);
    assertEquals(3, response.getSubscriptionsCount());
    assertEquals(SUBSCRIPTION1, response.getSubscriptions(0).getName());
    assertEquals(TOPIC1, response.getSubscriptions(0).getTopic());
    assertEquals(10, response.getSubscriptions(0).getAckDeadlineSeconds());

    assertEquals(SUBSCRIPTION3, response.getSubscriptions(1).getName());
    assertEquals(TOPIC2, response.getSubscriptions(1).getTopic());
    assertEquals(60, response.getSubscriptions(1).getAckDeadlineSeconds());

    assertEquals(SUBSCRIPTION2, response.getSubscriptions(2).getName());
    assertEquals(TOPIC1, response.getSubscriptions(2).getTopic());
    assertEquals(10, response.getSubscriptions(2).getAckDeadlineSeconds());

    assertTrue(response.getNextPageToken().isEmpty());
  }

  @Test
  public void listSubscriptionsWithPagination() {

    ListSubscriptionsResponse response =
        blockingStub.listSubscriptions(
            ListSubscriptionsRequest.newBuilder().setPageSize(1).build());

    assertEquals(1, response.getSubscriptionsCount());
    assertEquals(SUBSCRIPTION1, response.getSubscriptions(0).getName());
    assertEquals(TOPIC1, response.getSubscriptions(0).getTopic());
    assertEquals(10, response.getSubscriptions(0).getAckDeadlineSeconds());
    assertFalse(response.getNextPageToken().isEmpty());

    ListSubscriptionsResponse responseForSecondPage =
        blockingStub.listSubscriptions(
            ListSubscriptionsRequest.newBuilder()
                .setPageSize(1)
                .setPageToken(response.getNextPageToken())
                .build());

    assertEquals(1, responseForSecondPage.getSubscriptionsCount());
    assertEquals(SUBSCRIPTION3, responseForSecondPage.getSubscriptions(0).getName());
    assertEquals(TOPIC2, responseForSecondPage.getSubscriptions(0).getTopic());
    assertEquals(60, responseForSecondPage.getSubscriptions(0).getAckDeadlineSeconds());
    assertFalse(responseForSecondPage.getNextPageToken().isEmpty());

    ListSubscriptionsResponse responseForThirdPage =
        blockingStub.listSubscriptions(
            ListSubscriptionsRequest.newBuilder()
                .setPageSize(2)
                .setPageToken(responseForSecondPage.getNextPageToken())
                .build());

    assertEquals(1, responseForThirdPage.getSubscriptionsCount());
    assertEquals(SUBSCRIPTION2, responseForThirdPage.getSubscriptions(0).getName());
    assertEquals(TOPIC1, responseForThirdPage.getSubscriptions(0).getTopic());
    assertEquals(10, responseForThirdPage.getSubscriptions(0).getAckDeadlineSeconds());
  }

  @Test
  public void pullEmptyResponse() {
    PullRequest request =
        PullRequest.newBuilder().setSubscription(SUBSCRIPTION1).setMaxMessages(100).build();
    PullResponse response = blockingStub.pull(request);
    assertEquals(0, response.getReceivedMessagesCount());
  }

  @Test
  public void pullSubscriptionDoesNotExist() {
    try {
      PullRequest request =
          PullRequest.newBuilder()
              .setSubscription(SUBSCRIPTION_NOT_EXISTS)
              .setMaxMessages(100)
              .build();
      blockingStub.pull(request);
      fail("Subscription should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void pullSingleClient() {
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    PullRequest request =
        PullRequest.newBuilder().setSubscription(SUBSCRIPTION1).setMaxMessages(100).build();
    PullResponse response = blockingStub.pull(request);
    assertEquals(6, response.getReceivedMessagesCount());

    // Sort messageIds and data for easier evaluation
    List<String> ackIds =
        response
            .getReceivedMessagesList()
            .stream()
            .map(ReceivedMessage::getAckId)
            .sorted()
            .collect(Collectors.toList());
    List<String> messages =
        response
            .getReceivedMessagesList()
            .stream()
            .map(m -> m.getMessage().getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList());

    assertEquals(ackIds.get(0), "0-0");
    assertEquals(ackIds.get(1), "0-1");
    assertEquals(ackIds.get(2), "1-0");
    assertEquals(ackIds.get(3), "1-1");
    assertEquals(ackIds.get(4), "2-0");
    assertEquals(ackIds.get(5), "2-1");

    assertEquals(messages.get(0), "message-0000");
    assertEquals(messages.get(1), "message-0001");
    assertEquals(messages.get(2), "message-0002");
    assertEquals(messages.get(3), "message-0003");
    assertEquals(messages.get(4), "message-0004");
    assertEquals(messages.get(5), "message-0005");

    verify(statisticsManager, times(6))
        .computeSubscriber(
            eq(SUBSCRIPTION1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            any(Timestamp.class));
  }

  @Test
  public void pullSingleClientWithRequestBind() {
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(format(PROJECT_SUBSCRIPTION_FORMAT, PROJECT, SUBSCRIPTION1))
            .setMaxMessages(100)
            .build();
    PullResponse response = blockingStub.pull(request);
    assertEquals(6, response.getReceivedMessagesCount());

    // Sort messageIds and data for easier evaluation
    List<String> ackIds =
        response
            .getReceivedMessagesList()
            .stream()
            .map(ReceivedMessage::getAckId)
            .sorted()
            .collect(Collectors.toList());
    List<String> messages =
        response
            .getReceivedMessagesList()
            .stream()
            .map(m -> m.getMessage().getData().toString(TestHelpers.UTF8))
            .sorted()
            .collect(Collectors.toList());

    assertEquals(ackIds.get(0), "0-0");
    assertEquals(ackIds.get(1), "0-1");
    assertEquals(ackIds.get(2), "1-0");
    assertEquals(ackIds.get(3), "1-1");
    assertEquals(ackIds.get(4), "2-0");
    assertEquals(ackIds.get(5), "2-1");

    assertEquals(messages.get(0), "message-0000");
    assertEquals(messages.get(1), "message-0001");
    assertEquals(messages.get(2), "message-0002");
    assertEquals(messages.get(3), "message-0003");
    assertEquals(messages.get(4), "message-0004");
    assertEquals(messages.get(5), "message-0005");

    verify(statisticsManager, times(6))
        .computeSubscriber(
            eq(SUBSCRIPTION1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            any(Timestamp.class));
  }

  @Test
  public void pullSubscriptionRequestBindPathDoesNotExist() {
    try {
      PullRequest request =
          PullRequest.newBuilder()
              .setSubscription(format(PROJECT_SUBSCRIPTION_FORMAT, "not-exists", SUBSCRIPTION1))
              .setMaxMessages(100)
              .build();
      blockingStub.pull(request);
      fail("Subscription should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void pullConcurrentClients() {
    // TODO: Move scenario tests to a separate class?
    int partitions = 3;
    int recordsPerPartition = 2;
    int concurrentClients = 3;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CountDownLatch countDownLatch = new CountDownLatch(concurrentClients);
    ExecutorService executorService = Executors.newFixedThreadPool(concurrentClients);
    // Submit concurrentClients requests which will run concurrently once the latch is tripped
    List<Future<PullResponse>> futures = new ArrayList<>();
    for (int i = 0; i < concurrentClients; i++) {
      futures.add(
          executorService.submit(
              () -> {
                countDownLatch.countDown();
                countDownLatch.await();
                PullRequest request =
                    PullRequest.newBuilder()
                        .setSubscription(SUBSCRIPTION1)
                        .setMaxMessages(100)
                        .build();
                return blockingStub.pull(request);
              }));
    }

    // 6 messages should be retrieved with no duplication
    List<String> ackIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();
    for (Future<PullResponse> f : futures) {
      try {
        // One client should have gotten all messages, the others will receive empty lists
        PullResponse response = f.get();
        response
            .getReceivedMessagesList()
            .forEach(
                rm -> {
                  ackIds.add(rm.getAckId());
                  messages.add(rm.getMessage().getData().toString(TestHelpers.UTF8));
                });
      } catch (ExecutionException | InterruptedException e) {
        fail("Unexpected exception thrown " + e.getMessage());
      }
    }
    ackIds.sort(String::compareTo);
    assertEquals(6, ackIds.size());
    assertEquals(ackIds.get(0), "0-0");
    assertEquals(ackIds.get(1), "0-1");
    assertEquals(ackIds.get(2), "1-0");
    assertEquals(ackIds.get(3), "1-1");
    assertEquals(ackIds.get(4), "2-0");
    assertEquals(ackIds.get(5), "2-1");

    messages.sort(String::compareTo);
    assertEquals(6, messages.size());
    assertEquals(messages.get(0), "message-0000");
    assertEquals(messages.get(1), "message-0001");
    assertEquals(messages.get(2), "message-0002");
    assertEquals(messages.get(3), "message-0003");
    assertEquals(messages.get(4), "message-0004");
    assertEquals(messages.get(5), "message-0005");

    verify(statisticsManager, times(6))
        .computeSubscriber(
            eq(SUBSCRIPTION1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            any(Timestamp.class));
  }

  @Test
  public void acknowledge() {
    List<String> ackIds = Arrays.asList("0-0", "0-1", "0-2");
    AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder().setSubscription(SUBSCRIPTION1).addAllAckIds(ackIds).build();
    assertEquals(Empty.getDefaultInstance(), blockingStub.acknowledge(request));
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).acknowledge(ackIds);
  }

  @Test
  public void acknowledgeSubscriptionDoesNotExist() {
    try {
      AcknowledgeRequest request =
          AcknowledgeRequest.newBuilder()
              .setSubscription(SUBSCRIPTION_NOT_EXISTS)
              .addAllAckIds(Arrays.asList("0-0", "0-1", "0-2"))
              .build();
      blockingStub.acknowledge(request);
      fail("Subscription should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test
  public void modifyAckDeadline() {
    List<String> ackIds = Arrays.asList("0-0", "0-1", "0-2");
    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(SUBSCRIPTION1)
            .setAckDeadlineSeconds(300)
            .addAllAckIds(ackIds)
            .build();
    assertEquals(Empty.getDefaultInstance(), blockingStub.modifyAckDeadline(request));
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1))
        .modifyAckDeadline(ackIds, 300);
  }

  @Test
  public void modifyAckDeadlineSubscriptionDoesNotExist() {
    try {
      ModifyAckDeadlineRequest request =
          ModifyAckDeadlineRequest.newBuilder()
              .setSubscription(SUBSCRIPTION_NOT_EXISTS)
              .setAckDeadlineSeconds(300)
              .addAllAckIds(Arrays.asList("0-0", "0-1", "0-2"))
              .build();
      blockingStub.modifyAckDeadline(request);
      fail("Subscription should not exist");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
    } catch (Exception e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test(timeout = 10000)
  public void streamingPullSubscriptionDoesNotExist() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {}

              @Override
              public void onError(Throwable throwable) {
                finishLatch.countDown();
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder().setSubscription(SUBSCRIPTION_NOT_EXISTS).build();
    requestObserver.onNext(request);
    try {
      assertTrue(finishLatch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test(timeout = 10000)
  public void streamingPullSubscriptionWithRequestBindDoesNotExist() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {}

              @Override
              public void onError(Throwable throwable) {
                finishLatch.countDown();
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(format(PROJECT_SUBSCRIPTION_FORMAT, "not-exists", SUBSCRIPTION1))
            .build();
    requestObserver.onNext(request);
    try {
      assertTrue(finishLatch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  @Test(timeout = 10000)
  public void streamingPullCancel() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder().setSubscription(SUBSCRIPTION1).build();
    requestObserver.onNext(request);
    try {
      streamingFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
    requestObserver.onError(Status.CANCELLED.asException());
    waitForMessageReceipt();
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPullSingleClientPullAndAck() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<String> ackIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();

    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder().setSubscription(SUBSCRIPTION1).build();
    requestObserver.onNext(request);
    StreamingPullResponse response = null;
    try {
      response = streamingFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
    response
        .getReceivedMessagesList()
        .forEach(
            rm -> {
              ackIds.add(rm.getAckId());
              messages.add(rm.getMessage().getData().toString(TestHelpers.UTF8));
            });
    request = StreamingPullRequest.newBuilder().addAllAckIds(ackIds).build();
    requestObserver.onNext(request);

    assertEquals(6, ackIds.size());
    assertEquals(6, messages.size());
    List<String> sortedAckIds = ackIds.stream().sorted().collect(Collectors.toList());
    List<String> sortedMessages = messages.stream().sorted().collect(Collectors.toList());
    assertEquals(sortedAckIds.get(0), "0-0");
    assertEquals(sortedAckIds.get(1), "0-1");
    assertEquals(sortedAckIds.get(2), "1-0");
    assertEquals(sortedAckIds.get(3), "1-1");
    assertEquals(sortedAckIds.get(4), "2-0");
    assertEquals(sortedAckIds.get(5), "2-1");
    assertEquals(sortedMessages.get(0), "message-0000");
    assertEquals(sortedMessages.get(1), "message-0001");
    assertEquals(sortedMessages.get(2), "message-0002");
    assertEquals(sortedMessages.get(3), "message-0003");
    assertEquals(sortedMessages.get(4), "message-0004");
    assertEquals(sortedMessages.get(5), "message-0005");
    requestObserver.onCompleted();
    waitForMessageReceipt();
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).acknowledge(ackIds);
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1), atLeastOnce())
        .commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPullWithRequestBindSingleClientPullAndAck() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<String> ackIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();

    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(format(PROJECT_SUBSCRIPTION_FORMAT, PROJECT, SUBSCRIPTION1))
            .build();
    requestObserver.onNext(request);
    StreamingPullResponse response = null;
    try {
      response = streamingFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
    response
        .getReceivedMessagesList()
        .forEach(
            rm -> {
              ackIds.add(rm.getAckId());
              messages.add(rm.getMessage().getData().toString(TestHelpers.UTF8));
            });
    request = StreamingPullRequest.newBuilder().addAllAckIds(ackIds).build();
    requestObserver.onNext(request);

    assertEquals(6, ackIds.size());
    assertEquals(6, messages.size());
    List<String> sortedAckIds = ackIds.stream().sorted().collect(Collectors.toList());
    List<String> sortedMessages = messages.stream().sorted().collect(Collectors.toList());
    assertEquals(sortedAckIds.get(0), "0-0");
    assertEquals(sortedAckIds.get(1), "0-1");
    assertEquals(sortedAckIds.get(2), "1-0");
    assertEquals(sortedAckIds.get(3), "1-1");
    assertEquals(sortedAckIds.get(4), "2-0");
    assertEquals(sortedAckIds.get(5), "2-1");
    assertEquals(sortedMessages.get(0), "message-0000");
    assertEquals(sortedMessages.get(1), "message-0001");
    assertEquals(sortedMessages.get(2), "message-0002");
    assertEquals(sortedMessages.get(3), "message-0003");
    assertEquals(sortedMessages.get(4), "message-0004");
    assertEquals(sortedMessages.get(5), "message-0005");
    requestObserver.onCompleted();
    waitForMessageReceipt();
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).acknowledge(ackIds);
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1), atLeastOnce())
        .commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPullSingleClientPullDifferentAckDeadline() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());

    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(SUBSCRIPTION1)
            .setStreamAckDeadlineSeconds(30)
            .build();
    requestObserver.onNext(request);
    try {
      streamingFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
    requestObserver.onCompleted();
    waitForMessageReceipt();
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).pull(500, true, 30);
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1), atLeastOnce())
        .commitFromAcknowledgments();

    verify(statisticsManager, times(6))
        .computeSubscriber(
            eq(SUBSCRIPTION1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            any(Timestamp.class));
  }

  @Test(timeout = 10000)
  public void streamingPullSingleClientPullModifyAndAck() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<String> ackIds = new ArrayList<>();
    List<String> messages = new ArrayList<>();

    int partitions = 3;
    int recordsPerPartition = 2;
    MockConsumer<String, ByteBuffer> mockConsumer =
        kafkaClientFactory.getConsumersForSubscription(SUBSCRIPTION1).get(0);
    TestHelpers.generateConsumerRecords(TOPIC1, partitions, recordsPerPartition, null)
        .forEach(mockConsumer::addRecord);

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder().setSubscription(SUBSCRIPTION1).build();
    requestObserver.onNext(request);
    StreamingPullResponse response = null;
    try {
      response = streamingFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
    response
        .getReceivedMessagesList()
        .forEach(
            rm -> {
              ackIds.add(rm.getAckId());
              messages.add(rm.getMessage().getData().toString(TestHelpers.UTF8));
            });
    request =
        StreamingPullRequest.newBuilder()
            .addAllModifyDeadlineAckIds(ackIds.subList(0, 1))
            .addAllModifyDeadlineSeconds(Collections.singleton(60))
            .build();
    requestObserver.onNext(request);
    request = StreamingPullRequest.newBuilder().addAllAckIds(ackIds).build();
    requestObserver.onNext(request);

    assertEquals(6, ackIds.size());
    assertEquals(6, messages.size());
    List<String> sortedAckIds = ackIds.stream().sorted().collect(Collectors.toList());
    List<String> sortedMessages = messages.stream().sorted().collect(Collectors.toList());
    assertEquals(sortedAckIds.get(0), "0-0");
    assertEquals(sortedAckIds.get(1), "0-1");
    assertEquals(sortedAckIds.get(2), "1-0");
    assertEquals(sortedAckIds.get(3), "1-1");
    assertEquals(sortedAckIds.get(4), "2-0");
    assertEquals(sortedAckIds.get(5), "2-1");
    assertEquals(sortedMessages.get(0), "message-0000");
    assertEquals(sortedMessages.get(1), "message-0001");
    assertEquals(sortedMessages.get(2), "message-0002");
    assertEquals(sortedMessages.get(3), "message-0003");
    assertEquals(sortedMessages.get(4), "message-0004");
    assertEquals(sortedMessages.get(5), "message-0005");

    requestObserver.onCompleted();
    waitForMessageReceipt();
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1))
        .modifyAckDeadline(Collections.singletonList(ackIds.get(0)), 60);
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1)).acknowledge(ackIds);
    verify(subscriptionManageFactory.getForSubscription(SUBSCRIPTION1), atLeastOnce())
        .commitFromAcknowledgments();

    verify(statisticsManager, times(6))
        .computeSubscriber(
            eq(SUBSCRIPTION1),
            argThat(message -> message.toStringUtf8().matches(MESSAGE_CONTENT_REGEX)),
            any(Timestamp.class));
  }

  @Test(timeout = 10000)
  public void streamingPullMismatchedModifyAckDeadlines() {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());

    CompletableFuture<StreamingPullResponse> streamingFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                streamingFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                streamingFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder().setSubscription(SUBSCRIPTION1).build();
    requestObserver.onNext(request);
    request =
        StreamingPullRequest.newBuilder()
            .addAllModifyDeadlineAckIds(Arrays.asList("0-0", "0-1"))
            .addAllModifyDeadlineSeconds(Collections.singleton(60))
            .build();
    requestObserver.onNext(request);
    try {
      streamingFuture.get();
      fail("Expected StreamingPullResponse StreamObserver to throw Exception");
    } catch (ExecutionException e) {
      Status returned = Status.fromThrowable(e);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), returned.getCode());

    } catch (InterruptedException e) {
      fail("Unexpected exception thrown " + e.getMessage());
    }
  }

  /** Add a brief delay to allow for messages to reach the server */
  private void waitForMessageReceipt() {
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      System.out.println("Unexpected InterruptedException during wait");
    }
  }
}
