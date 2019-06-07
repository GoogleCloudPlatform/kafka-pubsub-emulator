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

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager.KAFKA_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import com.google.cloud.partners.pubsub.kafka.config.FakePubSubRepository;
import com.google.cloud.partners.pubsub.kafka.config.Project;
import com.google.cloud.partners.pubsub.kafka.config.PubSub;
import com.google.cloud.partners.pubsub.kafka.config.Topic;
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
import com.google.pubsub.v1.PushConfig;
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
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberServiceTest {

  @Rule public final GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private SubscriberGrpc.SubscriberBlockingStub blockingStub;
  private SubscriberService subscriber;

  private ConfigurationManager configurationManager;

  @Mock private KafkaClientFactory mockKafkaClientFactory;
  @Mock private SubscriptionManagerFactory mockSubscriptionManagerFactory;
  @Mock private SubscriptionManager mockSubscriptionManager1;
  @Mock private SubscriptionManager mockSubscriptionManager2;
  @Mock private SubscriptionManager mockSubscriptionManager3;
  @Mock private StatisticsManager mockStatisticsManager;
  private FakeSubscriptionManager fakeSubscriptionManager1;
  private FakeSubscriptionManager fakeSubscriptionManager2;
  private FakeSubscriptionManager fakeSubscriptionManager3;

  @Before
  public void setUp() {
    configurationManager = setupConfigurationRepository();
    subscriber =
        new SubscriberService(
            configurationManager, mockSubscriptionManagerFactory, mockStatisticsManager);
    grpcServerRule.getServiceRegistry().addService(subscriber);
    blockingStub = SubscriberGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void shutdown() {
    subscriber.shutdown();

    assertThat(fakeSubscriptionManager1.isRunning(), Matchers.is(false));
    assertThat(fakeSubscriptionManager2.isRunning(), Matchers.is(false));
    assertThat(fakeSubscriptionManager3.isRunning(), Matchers.is(false));
  }

  @Test
  public void createSubscription() {
    Subscription request =
        Subscription.newBuilder()
            .setTopic(TestHelpers.PROJECT1_TOPIC1)
            .setName("projects/project-1/subscriptions/new-subscriptions")
            .build();
    Subscription builtRequest =
        request
            .toBuilder()
            .setAckDeadlineSeconds(10)
            .putLabels(KAFKA_TOPIC, "kafka-topic-1")
            .setPushConfig(PushConfig.getDefaultInstance())
            .build();
    SubscriptionManager fakeSubscriptionManager =
        new FakeSubscriptionManager(
            builtRequest, mock(SubscriptionManager.class), mockKafkaClientFactory);
    when(mockSubscriptionManagerFactory.create(builtRequest)).thenReturn(fakeSubscriptionManager);

    assertThat(blockingStub.createSubscription(request), Matchers.equalTo(builtRequest));
    assertThat(fakeSubscriptionManager.isRunning(), Matchers.is(true));
    verify(mockSubscriptionManagerFactory).create(builtRequest);
  }

  @Test
  public void createSubscription_subscriptionExists() {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(Status.ALREADY_EXISTS.getCode().toString());

    Subscription request =
        Subscription.newBuilder()
            .setTopic(TestHelpers.PROJECT1_TOPIC1)
            .setName(TestHelpers.PROJECT1_SUBSCRIPTION1)
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
    String subscriptionName = TestHelpers.PROJECT1_SUBSCRIPTION1;
    DeleteSubscriptionRequest request =
        DeleteSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build();

    assertThat(fakeSubscriptionManager1.isRunning(), Matchers.is(true));
    blockingStub.deleteSubscription(request);
    assertThat(fakeSubscriptionManager1.isRunning(), Matchers.is(false));

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
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
            .build();
    Subscription response = blockingStub.getSubscription(request);
    assertThat(
        response,
        Matchers.equalTo(
            Subscription.newBuilder()
                .setName(TestHelpers.PROJECT1_SUBSCRIPTION1)
                .setTopic(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setPushConfig(PushConfig.getDefaultInstance())
                .setAckDeadlineSeconds(10)
                .build()));
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
        ListSubscriptionsRequest.newBuilder().setProject(TestHelpers.PROJECT1).build();
    ListSubscriptionsResponse response = blockingStub.listSubscriptions(request);

    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName(TestHelpers.PROJECT1_SUBSCRIPTION1)
                .setTopic(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setPushConfig(PushConfig.getDefaultInstance())
                .setAckDeadlineSeconds(10)
                .build(),
            Subscription.newBuilder()
                .setName(TestHelpers.PROJECT1_SUBSCRIPTION2)
                .setTopic(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setPushConfig(PushConfig.getDefaultInstance())
                .setAckDeadlineSeconds(30)
                .build()));
  }

  @Test
  public void listSubscriptions_withPagination() {
    ListSubscriptionsRequest request =
        ListSubscriptionsRequest.newBuilder()
            .setProject(TestHelpers.PROJECT1)
            .setPageSize(1)
            .build();
    ListSubscriptionsResponse response = blockingStub.listSubscriptions(request);

    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName(TestHelpers.PROJECT1_SUBSCRIPTION1)
                .setTopic(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setPushConfig(PushConfig.getDefaultInstance())
                .setAckDeadlineSeconds(10)
                .build()));
    assertThat(response.getNextPageToken(), Matchers.not(Matchers.isEmptyOrNullString()));

    request = request.toBuilder().setPageToken(response.getNextPageToken()).setPageSize(0).build();
    response = blockingStub.listSubscriptions(request);
    assertThat(
        response.getSubscriptionsList(),
        Matchers.contains(
            Subscription.newBuilder()
                .setName(TestHelpers.PROJECT1_SUBSCRIPTION2)
                .setTopic(TestHelpers.PROJECT1_TOPIC1)
                .putLabels(KAFKA_TOPIC, "kafka-topic-1")
                .setPushConfig(PushConfig.getDefaultInstance())
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
            .setSubscription(TestHelpers.PROJECT2_SUBSCRIPTION3)
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
            .setSubscription(TestHelpers.PROJECT2_SUBSCRIPTION3)
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
            .setSubscription(TestHelpers.PROJECT2_SUBSCRIPTION3)
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
            .setSubscription(TestHelpers.PROJECT2_SUBSCRIPTION3)
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
    CountDownLatch completeLatch = new CountDownLatch(1);
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
                completeLatch.countDown();
              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
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

    completeLatch.await();
    verify(mockSubscriptionManager1).acknowledge(ackIds);
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
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
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
  }

  @Test(timeout = 10000)
  public void streamingPull_overrideAckDeadline() throws ExecutionException, InterruptedException {
    CountDownLatch completeLatch = new CountDownLatch(1);
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
                completeLatch.countDown();
              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
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

    completeLatch.await();
    verify(mockSubscriptionManager1).acknowledge(ackIds);
  }

  @Test(timeout = 10000)
  public void streamingPull_modifyAndAck() throws ExecutionException, InterruptedException {
    CountDownLatch completeLatch = new CountDownLatch(1);
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
                completeLatch.countDown();
              }
            });
    StreamingPullRequest request =
        StreamingPullRequest.newBuilder()
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
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

    completeLatch.await();
    verify(mockSubscriptionManager1).modifyAckDeadline(Collections.singletonList("0-0"), 60);
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
            .setSubscription(TestHelpers.PROJECT1_SUBSCRIPTION1)
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

  @SuppressWarnings("unchecked")
  private ConfigurationManager setupConfigurationRepository() {
    configurationManager =
        new ConfigurationManager(
            TestHelpers.SERVER_CONFIG,
            new FakePubSubRepository(
                PubSub.newBuilder()
                    .addProjects(
                        Project.newBuilder()
                            .setName("project-1")
                            .addTopics(
                                Topic.newBuilder()
                                    .setName("topic-1")
                                    .setKafkaTopic("kafka-topic-1")
                                    .addSubscriptions(
                                        com.google.cloud.partners.pubsub.kafka.config.Subscription
                                            .newBuilder()
                                            .setName("subscription-1")
                                            .setAckDeadlineSeconds(10)
                                            .build())
                                    .addSubscriptions(
                                        com.google.cloud.partners.pubsub.kafka.config.Subscription
                                            .newBuilder()
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
                                        com.google.cloud.partners.pubsub.kafka.config.Subscription
                                            .newBuilder()
                                            .setName("subscription-3")
                                            .setAckDeadlineSeconds(45)
                                            .build())
                                    .build())
                            .build())
                    .build()));
    Subscription subscription1 =
        Subscription.newBuilder()
            .setName(TestHelpers.PROJECT1_SUBSCRIPTION1)
            .setTopic(TestHelpers.PROJECT1_TOPIC1)
            .putLabels(KAFKA_TOPIC, "kafka-topic-1")
            .setPushConfig(PushConfig.getDefaultInstance())
            .setAckDeadlineSeconds(10)
            .build();
    Subscription subscription2 =
        Subscription.newBuilder()
            .setName(TestHelpers.PROJECT1_SUBSCRIPTION2)
            .setTopic(TestHelpers.PROJECT1_TOPIC1)
            .putLabels(KAFKA_TOPIC, "kafka-topic-1")
            .setPushConfig(PushConfig.getDefaultInstance())
            .setAckDeadlineSeconds(30)
            .build();
    Subscription subscription3 =
        Subscription.newBuilder()
            .setName(TestHelpers.PROJECT2_SUBSCRIPTION3)
            .setTopic("projects/project-2/topics/topic-2")
            .putLabels(KAFKA_TOPIC, "kafka-topic-2")
            .setPushConfig(PushConfig.getDefaultInstance())
            .setAckDeadlineSeconds(45)
            .build();

    fakeSubscriptionManager1 =
        spy(
            new FakeSubscriptionManager(
                subscription1, mockSubscriptionManager1, mockKafkaClientFactory));
    fakeSubscriptionManager2 =
        spy(
            new FakeSubscriptionManager(
                subscription2, mockSubscriptionManager2, mockKafkaClientFactory));
    fakeSubscriptionManager3 =
        spy(
            new FakeSubscriptionManager(
                subscription3, mockSubscriptionManager3, mockKafkaClientFactory));

    when(mockSubscriptionManagerFactory.create(subscription1)).thenReturn(fakeSubscriptionManager1);
    when(mockSubscriptionManagerFactory.create(subscription2)).thenReturn(fakeSubscriptionManager2);
    when(mockSubscriptionManagerFactory.create(subscription3)).thenReturn(fakeSubscriptionManager3);

    return configurationManager;
  }

  // Making a fake class that wraps a Mock
  private static class FakeSubscriptionManager extends SubscriptionManager {

    private final SubscriptionManager mockDelegate;
    private final Subscription subscription;

    FakeSubscriptionManager(
        Subscription subscription,
        SubscriptionManager mockDelegate,
        KafkaClientFactory kafkaClientFactory) {
      super(subscription, kafkaClientFactory, Clock.systemUTC(), 4);
      this.mockDelegate = mockDelegate;
      this.subscription = subscription;
    }

    @SuppressWarnings("unchecked")
    private static KafkaClientFactory configureMockKafkaClientFactory() {
      Consumer<String, ByteBuffer> mockConsumer = mock(Consumer.class);
      KafkaClientFactory kafkaClientFactory = mock(KafkaClientFactory.class);
      when(kafkaClientFactory.createConsumer(anyString())).thenReturn(mockConsumer);
      when(mockConsumer.partitionsFor(anyString())).thenReturn(Collections.emptyList());
      return kafkaClientFactory;
    }

    @Override
    public Subscription getSubscription() {
      return subscription;
    }

    @Override
    public List<PubsubMessage> pull(int maxMessages, boolean returnImmediately) {
      return mockDelegate.pull(maxMessages, returnImmediately);
    }

    @Override
    public List<PubsubMessage> pull(
        int maxMessages, boolean returnImmediately, int ackDeadlineSecs) {
      return mockDelegate.pull(maxMessages, returnImmediately, ackDeadlineSecs);
    }

    @Override
    public List<String> acknowledge(List<String> ackIds) {
      return mockDelegate.acknowledge(ackIds);
    }

    @Override
    public List<String> modifyAckDeadline(List<String> ackIds, int ackDeadlineSecs) {
      return mockDelegate.modifyAckDeadline(ackIds, ackDeadlineSecs);
    }

    @Override
    public String toString() {
      return mockDelegate.toString();
    }

    @Override
    protected void startUp() {
      // Noop
    }
  }
}
