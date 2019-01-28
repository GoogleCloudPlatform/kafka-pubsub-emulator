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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.partners.pubsub.kafka.config.Configuration;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.cloud.partners.pubsub.kafka.config.FakeConfigurationRepository;
import com.google.cloud.partners.pubsub.kafka.config.Kafka;
import com.google.cloud.partners.pubsub.kafka.config.Project;
import com.google.cloud.partners.pubsub.kafka.config.PubSub;
import com.google.cloud.partners.pubsub.kafka.config.Server;
import com.google.cloud.partners.pubsub.kafka.config.Topic;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.zookeeper.data.Stat;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberServiceTest {

  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private SubscriberGrpc.SubscriberBlockingStub blockingStub;
  private SubscriberService subscriber;
  private CountDownLatch streamingPullCommitLatch;

  private ConfigurationRepository configurationRepository;

  @Mock private KafkaClientFactory mockKafkaClientFactory;
  @Mock private SubscriptionManagerFactory mockSubscriptionManagerFactory;
  @Mock private SubscriptionManager mockSubscriptionManager1;
  @Mock private SubscriptionManager mockSubscriptionManager2;
  @Mock private SubscriptionManager mockSubscriptionManager3;
  @Mock private StatisticsManager mockStatisticsManager;

  @Before
  public void setUp() {
    streamingPullCommitLatch = new CountDownLatch(1);
    configurationRepository = setupConfigurationRepository();
    subscriber =
        new SubscriberService(
            configurationRepository,
            mockKafkaClientFactory,
            mockSubscriptionManagerFactory,
            mockStatisticsManager);
    grpcServerRule.getServiceRegistry().addService(subscriber);
    blockingStub = SubscriberGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void shutdown() {
    subscriber.shutdown();
    //    kafkaClientFactory
    //        .getConsumersForSubscription(SUBSCRIPTION1)
    //        .forEach(c -> assertTrue(c.closed()));
    //    kafkaClientFactory
    //        .getConsumersForSubscription(SUBSCRIPTION2)
    //        .forEach(c -> assertTrue(c.closed()));
    //    kafkaClientFactory
    //        .getConsumersForSubscription(SUBSCRIPTION3)
    //        .forEach(c -> assertTrue(c.closed()));
  }

  @Test
  public void createSubscription() {
    Subscription request =
        Subscription.newBuilder()
            .setTopic("projects/project-1/topics/topic-1")
            .setName("projects/project-1/subscriptions/new-subscriptions")
            .build();
    when(mockSubscriptionManagerFactory.create(
            eq(request), eq(mockKafkaClientFactory), any(ScheduledThreadPoolExecutor.class)))
        .thenReturn(mock(SubscriptionManager.class));

    assertThat(blockingStub.createSubscription(request), Matchers.equalTo(request));
    //    verify(mockSubscriptionManagerFactory)
    //        .create(request, mockKafkaClientFactory, any(ScheduledThreadPoolExecutor.class));
  }

  @Test
  public void createSubscription_subscriptionExists() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.ALREADY_EXISTS.getCode().toString());

    Subscription request =
        Subscription.newBuilder()
            .setTopic("projects/project-1/topics/topic-1")
            .setName("projects/project-1/subscriptions/subscription-1")
            .build();
    blockingStub.createSubscription(request);
  }

  @Test
  public void createSubscription_topicDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    Subscription request =
        Subscription.newBuilder()
            .setTopic("projects/project-1/topics/unknown-topic")
            .setName("projects/project-1/subscriptions/new-subscription")
            .build();
    blockingStub.createSubscription(request);
  }

  @Test
  public void deleteSubscription() {
    String subscriptionName = "projects/project-1/subscriptions/subscription-1";
    DeleteSubscriptionRequest request =
        DeleteSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build();

    blockingStub.deleteSubscription(request);

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    blockingStub.getSubscription(
        GetSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build());
  }

  @Test
  public void deleteSubscription_subscriptionDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    String subscriptionName = "projects/project-1/subscriptions/unknown-subscription";
    DeleteSubscriptionRequest request =
        DeleteSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build();

    blockingStub.deleteSubscription(request);
  }

  @Test
  public void getSubscription() {
    GetSubscriptionRequest request =
        GetSubscriptionRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .build();
    Subscription response = blockingStub.getSubscription(request);
    assertThat(request, Matchers.equalTo(request));
  }

  @Test
  public void getSubscription_subscriptionDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    GetSubscriptionRequest request =
        GetSubscriptionRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/unknown-subscription")
            .build();
    blockingStub.getSubscription(request);
  }

  @Test
  public void listSubscriptions() {
    ListSubscriptionsRequest request =
        ListSubscriptionsRequest.newBuilder().setProject("projects/project-1").build();
    ListSubscriptionsResponse response = blockingStub.listSubscriptions(request);

    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build(),
            Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(30)
                .build()));
  }

  @Test
  public void listSubscriptions_withPagination() {
    ListSubscriptionsRequest request =
        ListSubscriptionsRequest.newBuilder()
            .setProject("projects/project-1")
            .setPageSize(1)
            .build();
    ListSubscriptionsResponse response = blockingStub.listSubscriptions(request);

    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-1")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(10)
                .build()));
    assertThat(response.getNextPageToken(), Matchers.not(Matchers.isEmptyOrNullString()));

    request = request.toBuilder().setPageToken(response.getNextPageToken()).setPageSize(0).build();
    response = blockingStub.listSubscriptions(request);
    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName("projects/project-1/subscriptions/subscription-2")
                .setTopic("projects/project-1/topics/topic-1")
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setAckDeadlineSeconds(30)
                .build()));
    assertThat(response.getNextPageToken(), Matchers.isEmptyOrNullString());
  }

  @Test
  public void pull() {
    List<PubsubMessage> messages =
        Arrays.asList(
            PubsubMessage.newBuilder()
                .setMessageId("0-0")
                .setData(ByteString.copyFromUtf8("hello"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-1")
                .setData(ByteString.copyFromUtf8("world"))
                .build());
    when(mockSubscriptionManager3.pull(100, false)).thenReturn(messages);

    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription("projects/project-2/subscriptions/subscription-3")
            .setMaxMessages(100)
            .build();
    PullResponse response = blockingStub.pull(request);
    assertThat(
        response.getReceivedMessagesList(),
        Matchers.contains(
            ReceivedMessage.newBuilder().setAckId("0-0").setMessage(messages.get(0)).build(),
            ReceivedMessage.newBuilder().setAckId("0-1").setMessage(messages.get(1)).build()));
  }

  @Test
  public void pull_subscriptionDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/unknown-subscription")
            .setMaxMessages(100)
            .build();
    blockingStub.pull(request);
  }

  @Test
  public void pull_emptyList() {
    when(mockSubscriptionManager3.pull(100, false)).thenReturn(Collections.emptyList());

    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription("projects/project-2/subscriptions/subscription-3")
            .setMaxMessages(100)
            .build();
    PullResponse response = blockingStub.pull(request);

    assertThat(response.getReceivedMessagesList(), Matchers.empty());
  }

  @Test
  public void acknowledge() {
    List<String> ackIds = Arrays.asList("0-0", "0-1", "0-2");
    when(mockSubscriptionManager3.acknowledge(ackIds)).thenReturn(ackIds);

    AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription("projects/project-2/subscriptions/subscription-3")
            .addAllAckIds(ackIds)
            .build();
    assertThat(blockingStub.acknowledge(request), Matchers.equalTo(Empty.getDefaultInstance()));

    verify(mockSubscriptionManager3).acknowledge(ackIds);
  }

  @Test
  public void acknowledge_subscriptionDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/unknown-subscription")
            .addAllAckIds(Arrays.asList("0-0", "0-1", "0-2"))
            .build();
    blockingStub.acknowledge(request);
  }

  @Test
  public void modifyAckDeadline() {
    List<String> ackIds = Arrays.asList("0-0", "0-1", "0-2");
    when(mockSubscriptionManager3.modifyAckDeadline(ackIds, 300)).thenReturn(ackIds);

    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription("projects/project-2/subscriptions/subscription-3")
            .setAckDeadlineSeconds(300)
            .addAllAckIds(ackIds)
            .build();
    assertThat(
        blockingStub.modifyAckDeadline(request), Matchers.equalTo(Empty.getDefaultInstance()));

    verify(mockSubscriptionManager3).modifyAckDeadline(ackIds, 300);
  }

  @Test
  public void modifyAckDeadline_subscriptionDoesNotExist() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.NOT_FOUND.getCode().toString());

    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/unknown-subscription")
            .setAckDeadlineSeconds(300)
            .addAllAckIds(Arrays.asList("0-0", "0-1", "0-2"))
            .build();
    blockingStub.modifyAckDeadline(request);
  }

  @Test(timeout = 10000)
  public void streamingPull() throws ExecutionException, InterruptedException {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<PubsubMessage> messages =
        Arrays.asList(
            PubsubMessage.newBuilder()
                .setMessageId("0-0")
                .setData(ByteString.copyFromUtf8("hello"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-1")
                .setData(ByteString.copyFromUtf8("world"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-2")
                .setData(ByteString.copyFromUtf8("goodbye"))
                .build());
    when(mockSubscriptionManager1.pull(500, true, 10)).thenReturn(messages);

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
              public void onCompleted() {

              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .build();
    requestObserver.onNext(request);

    StreamingPullResponse response = streamingFuture.get();
    assertThat(
        response.getReceivedMessagesList(),
        Matchers.contains(
            ReceivedMessage.newBuilder().setAckId("0-0").setMessage(messages.get(0)).build(),
            ReceivedMessage.newBuilder().setAckId("0-1").setMessage(messages.get(1)).build(),
            ReceivedMessage.newBuilder().setAckId("0-2").setMessage(messages.get(2)).build()));

    List<String> ackIds =
        response
            .getReceivedMessagesList()
            .stream()
            .map(ReceivedMessage::getAckId)
            .collect(Collectors.toList());

    request = StreamingPullRequest.newBuilder().addAllAckIds(ackIds).build();
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    streamingPullCommitLatch.await();

    verify(mockSubscriptionManager1).acknowledge(ackIds);
    verify(mockSubscriptionManager1, atLeastOnce()).commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPull_subscriptionDoesNotExist() throws InterruptedException {
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
            .setSubscription("projects/project-1/subscriptions/unknown-subscription")
            .build();
    requestObserver.onNext(request);
    assertThat(finishLatch.await(5, TimeUnit.SECONDS), Matchers.is(true));
  }

  @Test(timeout = 10000)
  public void streamingPull_cancel() throws ExecutionException, InterruptedException {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    when(mockSubscriptionManager1.pull(500, true, 10)).thenReturn(Collections.emptyList());

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
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .build();
    requestObserver.onNext(request);
    try {
      streamingFuture.get(2, TimeUnit.SECONDS); // Wait before canceling
    } catch (TimeoutException ignored) {
    }

    expectedException.expect(ExecutionException.class);
    expectedException.expectMessage(Status.CANCELLED.getCode().toString());

    requestObserver.onError(Status.CANCELLED.asException());
    streamingFuture.get();
    streamingPullCommitLatch.await();

    verify(mockSubscriptionManager1, atLeastOnce()).commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPull_overrideAckDeadline() throws ExecutionException, InterruptedException {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<PubsubMessage> messages =
        Arrays.asList(
            PubsubMessage.newBuilder()
                .setMessageId("0-0")
                .setData(ByteString.copyFromUtf8("hello"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-1")
                .setData(ByteString.copyFromUtf8("world"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-2")
                .setData(ByteString.copyFromUtf8("goodbye"))
                .build());
    when(mockSubscriptionManager1.pull(500, true, 60)).thenReturn(messages);

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
              public void onCompleted() {

              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .setStreamAckDeadlineSeconds(60)
            .build();
    requestObserver.onNext(request);

    StreamingPullResponse response = streamingFuture.get();
    assertThat(
        response.getReceivedMessagesList(),
        Matchers.contains(
            ReceivedMessage.newBuilder().setAckId("0-0").setMessage(messages.get(0)).build(),
            ReceivedMessage.newBuilder().setAckId("0-1").setMessage(messages.get(1)).build(),
            ReceivedMessage.newBuilder().setAckId("0-2").setMessage(messages.get(2)).build()));

    List<String> ackIds =
        response
            .getReceivedMessagesList()
            .stream()
            .map(ReceivedMessage::getAckId)
            .collect(Collectors.toList());

    request = StreamingPullRequest.newBuilder().addAllAckIds(ackIds).build();
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    streamingPullCommitLatch.await();

    verify(mockSubscriptionManager1).acknowledge(ackIds);
    verify(mockSubscriptionManager1, atLeastOnce()).commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPull_modifyAndAck() throws ExecutionException, InterruptedException {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<PubsubMessage> messages =
        Arrays.asList(
            PubsubMessage.newBuilder()
                .setMessageId("0-0")
                .setData(ByteString.copyFromUtf8("hello"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-1")
                .setData(ByteString.copyFromUtf8("world"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-2")
                .setData(ByteString.copyFromUtf8("goodbye"))
                .build());
    when(mockSubscriptionManager1.pull(500, true, 10)).thenReturn(messages);

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
              public void onCompleted() {

              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .build();
    requestObserver.onNext(request);

    StreamingPullResponse response = streamingFuture.get();
    assertThat(
        response.getReceivedMessagesList(),
        Matchers.contains(
            ReceivedMessage.newBuilder().setAckId("0-0").setMessage(messages.get(0)).build(),
            ReceivedMessage.newBuilder().setAckId("0-1").setMessage(messages.get(1)).build(),
            ReceivedMessage.newBuilder().setAckId("0-2").setMessage(messages.get(2)).build()));

    request =
        StreamingPullRequest.newBuilder()
            .addAllModifyDeadlineAckIds(Collections.singletonList("0-0"))
            .addAllModifyDeadlineSeconds(Collections.singleton(60))
            .build();
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    streamingPullCommitLatch.await();

    verify(mockSubscriptionManager1).modifyAckDeadline(Collections.singletonList("0-0"), 60);
    verify(mockSubscriptionManager1, atLeastOnce()).commitFromAcknowledgments();
  }

  @Test(timeout = 10000)
  public void streamingPull_mismatchedModifyAckDeadlines()
      throws ExecutionException, InterruptedException {
    SubscriberStub asyncStub = SubscriberGrpc.newStub(grpcServerRule.getChannel());
    List<PubsubMessage> messages =
        Arrays.asList(
            PubsubMessage.newBuilder()
                .setMessageId("0-0")
                .setData(ByteString.copyFromUtf8("hello"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-1")
                .setData(ByteString.copyFromUtf8("world"))
                .build(),
            PubsubMessage.newBuilder()
                .setMessageId("0-2")
                .setData(ByteString.copyFromUtf8("goodbye"))
                .build());
    when(mockSubscriptionManager1.pull(500, true, 10)).thenReturn(messages);

    CompletableFuture<StreamingPullResponse> messagesFuture = new CompletableFuture<>();
    CompletableFuture<StreamingPullResponse> errorFuture = new CompletableFuture<>();
    StreamObserver<StreamingPullRequest> requestObserver =
        asyncStub.streamingPull(
            new StreamObserver<StreamingPullResponse>() {
              @Override
              public void onNext(StreamingPullResponse streamingPullResponse) {
                messagesFuture.complete(streamingPullResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                errorFuture.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription("projects/project-1/subscriptions/subscription-1")
            .build();
    requestObserver.onNext(request);

    StreamingPullResponse response = messagesFuture.get();
    assertThat(
        response.getReceivedMessagesList(),
        Matchers.contains(
            ReceivedMessage.newBuilder().setAckId("0-0").setMessage(messages.get(0)).build(),
            ReceivedMessage.newBuilder().setAckId("0-1").setMessage(messages.get(1)).build(),
            ReceivedMessage.newBuilder().setAckId("0-2").setMessage(messages.get(2)).build()));

    request =
        StreamingPullRequest.newBuilder()
            .addAllModifyDeadlineAckIds(Arrays.asList("0-0", "0-1"))
            .addAllModifyDeadlineSeconds(Collections.singleton(60))
            .build();

    expectedException.expect(ExecutionException.class);
    expectedException.expectMessage(
        "Request contained 2 modifyAckDeadlineIds but 1 modifyDeadlineSeconds");

    requestObserver.onNext(request);
    errorFuture.get();
  }

  private ConfigurationRepository setupConfigurationRepository() {
    configurationRepository =
        new FakeConfigurationRepository(
            Configuration.newBuilder()
                .setServer(Server.newBuilder().setPort(8080).build())
                .setKafka(
                    Kafka.newBuilder()
                        .addAllBootstrapServers(ImmutableList.of("server1:2192", "server2:2192"))
                        .putProducerProperties("max.poll.records", "1000")
                        .putConsumerProperties("linger.ms", "5")
                        .putConsumerProperties("batch.size", "1000000")
                        .putConsumerProperties("buffer.memory", "32000000")
                        .setProducerExecutors(4)
                        .setConsumersPerSubscription(4)
                        .build())
                .setPubsub(
                    PubSub.newBuilder()
                        .addProjects(
                            Project.newBuilder()
                                .setName("project-1")
                                .addTopics(
                                    Topic.newBuilder()
                                        .setName("topic-1")
                                        .setKafkaTopic("kafka-topic-1")
                                        .addSubscriptions(
                                            com.google.cloud.partners.pubsub.kafka.config
                                                .Subscription.newBuilder()
                                                .setName("subscription-1")
                                                .setAckDeadlineSeconds(10)
                                                .build())
                                        .addSubscriptions(
                                            com.google.cloud.partners.pubsub.kafka.config
                                                .Subscription.newBuilder()
                                                .setName("subscription-2")
                                                .setAckDeadlineSeconds(30)
                                                .build())
                                        .build())
                                .build())
                        .addProjects(
                            Project.newBuilder()
                                .setName("project-2")
                                .addTopics(
                                    Topic.newBuilder()
                                        .setName("topic-2")
                                        .setKafkaTopic("kafka-topic-2")
                                        .addSubscriptions(
                                            com.google.cloud.partners.pubsub.kafka.config
                                                .Subscription.newBuilder()
                                                .setName("subscription-3")
                                                .setAckDeadlineSeconds(45)
                                                .build())
                                        .build())
                                .build()))
                .build());
    Subscription subscription1 =
        Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/subscription-1")
            .setTopic("projects/project-1/topics/topic-1")
            .putLabels(KAFKA_TOPIC, "kafka-topic-1")
            .setAckDeadlineSeconds(10)
            .build();
    Subscription subscription2 =
        Subscription.newBuilder()
            .setName("projects/project-1/subscriptions/subscription-2")
            .setTopic("projects/project-1/topics/topic-1")
            .putLabels(KAFKA_TOPIC, "kafka-topic-1")
            .setAckDeadlineSeconds(30)
            .build();
    Subscription subscription3 =
        Subscription.newBuilder()
            .setName("projects/project-2/subscriptions/subscription-3")
            .setTopic("projects/project-2/topics/topic-2")
            .putLabels(KAFKA_TOPIC, "kafka-topic-2")
            .setAckDeadlineSeconds(45)
            .build();

    when(mockSubscriptionManagerFactory.create(
            eq(subscription1), eq(mockKafkaClientFactory), any(ScheduledThreadPoolExecutor.class)))
        .thenReturn(mockSubscriptionManager1);
    when(mockSubscriptionManagerFactory.create(
            eq(subscription2), eq(mockKafkaClientFactory), any(ScheduledThreadPoolExecutor.class)))
        .thenReturn(mockSubscriptionManager2);
    when(mockSubscriptionManagerFactory.create(
            eq(subscription3), eq(mockKafkaClientFactory), any(ScheduledThreadPoolExecutor.class)))
        .thenReturn(mockSubscriptionManager3);

    when(mockSubscriptionManager1.getSubscription()).thenReturn(subscription1);
    when(mockSubscriptionManager1.commitFromAcknowledgments())
        .thenAnswer(
            invocationOnMock -> {
              streamingPullCommitLatch.countDown();
              return Collections.emptyMap();
            });

    return configurationRepository;
  }
}
