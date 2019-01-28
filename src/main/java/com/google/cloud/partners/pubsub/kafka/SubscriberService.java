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

import static java.util.Objects.isNull;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationAlreadyExistsException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationNotFoundException;
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
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Implementation of <a
 * href="https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#subscriber"
 * target="_blank"> Cloud Pub/Sub Publisher API.</a>
 */
class SubscriberService extends SubscriberImplBase {

  private static final Logger LOGGER = Logger.getLogger(SubscriberService.class.getName());
  private static final AtomicInteger STREAMING_PULL_ID = new AtomicInteger();

  private final ScheduledExecutorService commitExecutorService;
  private final ExecutorService streamingPullExecutorService;
  private final Map<String, SubscriptionManager> subscriptions;
  private final StatisticsManager statisticsManager;
  private final ConfigurationRepository configurationRepository;
  private final SubscriptionManagerFactory subscriptionManagerFactory;
  private final KafkaClientFactory kafkaClientFactory;

  SubscriberService(
      ConfigurationRepository configurationRepository,
      KafkaClientFactory kafkaClientFactory,
      SubscriptionManagerFactory subscriptionManagerFactory,
      StatisticsManager statisticsManager) {
    this.configurationRepository = configurationRepository;
    this.statisticsManager = statisticsManager;
    this.subscriptionManagerFactory = subscriptionManagerFactory;
    this.kafkaClientFactory = kafkaClientFactory;

    commitExecutorService =
        Executors.newScheduledThreadPool(
            configurationRepository.getKafka().getConsumersPerSubscription(),
            Utils.newThreadFactoryWithGroupAndPrefix(
                "subscriber-commit-threads", "subscriber-committer"));
    streamingPullExecutorService =
        Executors.newCachedThreadPool(
            Utils.newThreadFactoryWithGroupAndPrefix("streaming-pull-threads", "streaming-puller"));

    subscriptions =
        configurationRepository
            .getProjects()
            .stream()
            .flatMap(p -> configurationRepository.getSubscriptions(p).stream())
            .collect(
                Collectors.toConcurrentMap(
                    Subscription::getName,
                    s ->
                        subscriptionManagerFactory.create(
                            s, kafkaClientFactory, commitExecutorService)));
    LOGGER.info("Created " + subscriptions.size() + " SubscriptionManagers");
  }

  /**
   * Shutdown hook halts the scheduled executor service preventing new commit tasks from being
   * submitted, executes commits for each {@link SubscriptionManager}, and then closes all
   * KafkaConsumers.
   */
  public void shutdown() {
    commitExecutorService.shutdown();
    subscriptions.values().forEach(SubscriptionManager::shutdown);
  }

  @Override
  public void createSubscription(
      Subscription request, StreamObserver<Subscription> responseObserver) {
    try {
      configurationRepository.createSubscription(request);
      subscriptions.put(
          request.getName(),
          subscriptionManagerFactory.create(request, kafkaClientFactory, commitExecutorService));
      // statisticsManager.addSubscriberInformation(subscriptionProperties);
      responseObserver.onNext(request);
      responseObserver.onCompleted();
    } catch (ConfigurationAlreadyExistsException e) {
      responseObserver.onError(Status.ALREADY_EXISTS.withCause(e).asException());
    } catch (ConfigurationNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
    }
  }

  @Override
  public void deleteSubscription(
      DeleteSubscriptionRequest request, StreamObserver<Empty> responseObserver) {
    try {
      configurationRepository.deleteSubscription(request.getSubscription());
      //      // verify if not contains anymore topic remove subscriber information from statistics.
      //      if (!kafkaProperties.getTopics().contains(topic)) {
      //        statisticsManager.removeSubscriberInformation(topic);
      //      }

      // remove from subscriptions map
      subscriptions.get(request.getSubscription()).shutdown();
      subscriptions.remove(request.getSubscription());
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (ConfigurationNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
    }
  }

  @Override
  public void listSubscriptions(
      ListSubscriptionsRequest request,
      StreamObserver<ListSubscriptionsResponse> responseObserver) {
    PaginationManager<Subscription> paginationManager =
        new PaginationManager<>(
            configurationRepository.getSubscriptions(request.getProject()), Subscription::getName);
    ListSubscriptionsResponse response =
        ListSubscriptionsResponse.newBuilder()
            .addAllSubscriptions(
                paginationManager.paginate(request.getPageSize(), request.getPageToken()))
            .setNextPageToken(paginationManager.getNextToken(Subscription::getName))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getSubscription(
      GetSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
    Optional<Subscription> subscription =
        configurationRepository.getSubscriptionByName(request.getSubscription());
    if (!subscription.isPresent()) {
      String message = request.getSubscription() + " is not a valid Subscription";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      responseObserver.onNext(subscription.get());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
    SubscriptionManager subscriptionManager = subscriptions.get(request.getSubscription());
    if (subscriptionManager == null) {
      String message = request.getSubscription() + " is not a valid Subscription";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      PullResponse response =
          PullResponse.newBuilder()
              .addAllReceivedMessages(
                  buildReceivedMessageList(
                      subscriptionManager.pull(
                          request.getMaxMessages(), request.getReturnImmediately())))
              .build();
      LOGGER.fine("Returning " + response.getReceivedMessagesCount() + " messages");
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
    SubscriptionManager subscriptionManager = subscriptions.get(request.getSubscription());
    if (subscriptionManager == null) {
      String message = request.getSubscription() + " is not a valid Subscription";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      List<String> ackIds = subscriptionManager.acknowledge(request.getAckIdsList());
      LOGGER.fine("Successfully acknowledged " + ackIds.size() + " messages");
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void modifyAckDeadline(
      ModifyAckDeadlineRequest request, StreamObserver<Empty> responseObserver) {
    SubscriptionManager subscriptionManager = subscriptions.get(request.getSubscription());
    if (subscriptionManager == null) {
      String message = request.getSubscription() + " is not a valid Subscription";
      LOGGER.warning(message);
      responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException());
    } else {
      List<String> ackIds =
          subscriptionManager.modifyAckDeadline(
              request.getAckIdsList(), request.getAckDeadlineSeconds());
      LOGGER.fine("Successfully modified ack deadline for " + ackIds.size() + " messages");
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  @Override
  public StreamObserver<StreamingPullRequest> streamingPull(
      StreamObserver<StreamingPullResponse> responseObserver) {
    return new StreamingPullStreamObserver(responseObserver);
  }

  private List<ReceivedMessage> buildReceivedMessageList(List<PubsubMessage> pubsubMessages) {
    // TODO: Add stats collection
    // statisticsManager.computeSubscriber(
    //                                    subscription,
    //                                    receivedMessage.getMessage().getData(),
    //                                    receivedMessage.getMessage().getPublishTime());
    return pubsubMessages
        .stream()
        .map(m -> ReceivedMessage.newBuilder().setAckId(m.getMessageId()).setMessage(m).build())
        .collect(Collectors.toList());
  }

  /**
   * Implementation of the Subscriber StreamingPull bidi streaming method. Modeled after the
   * implementation found in the Cloud Pub/Sub emulator available in the gcloud CLI tool.
   */
  private final class StreamingPullStreamObserver implements StreamObserver<StreamingPullRequest> {

    private static final int MESSAGES_PER_STREAM = 500;
    private static final int MIN_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 2048;

    private final CompletableFuture<Void> terminationFuture;
    private final ServerCallStreamObserver<StreamingPullResponse> responseObserver;
    private int streamAckDeadlineSecs;
    private String streamId;
    private SubscriptionManager subscriptionManager;

    StreamingPullStreamObserver(StreamObserver<StreamingPullResponse> responseObserver) {
      // Upcast to a ServerCallStreamObserver to set manual flow control
      this.responseObserver = (ServerCallStreamObserver<StreamingPullResponse>) responseObserver;
      this.responseObserver.disableAutoInboundFlowControl();

      terminationFuture = new CompletableFuture<>();

      this.responseObserver.setOnReadyHandler(
          () -> {
            if (isNull(subscriptionManager)) {
              this.responseObserver.request(1);
            }
          });
      this.responseObserver.setOnCancelHandler(
          () -> {
            LOGGER.info("Client cancelled StreamingPull " + streamId);
            terminationFuture.complete(null);
          });
    }

    @Override
    public void onNext(StreamingPullRequest request) {
      try {
        processSubscription(request);
        processAcks(request);
        processModifyAckDeadlines(request);
        responseObserver.request(1);
      } catch (StatusException e) {
        terminationFuture.completeExceptionally(e);
        responseObserver.onError(Status.fromThrowable(e).asException());
      }
    }

    @Override
    public void onError(Throwable throwable) {
      // This doesn't seem to occur given that the standard client uses a cancel message onError
      if (!Status.fromThrowable(throwable).getCode().equals(Status.CANCELLED.getCode())) {
        LOGGER.warning(
            "Client encountered error during StreamingPull "
                + streamId
                + ": "
                + throwable.getMessage());
        terminationFuture.completeExceptionally(throwable);
      }
    }

    @Override
    public void onCompleted() {
      LOGGER.info("StreamingPull " + streamId + " closed by client");
      terminationFuture.complete(null);
      responseObserver.onCompleted();
    }

    /**
     * Handles the initial client request in the stream which sets up the {@code
     * SubscriptionManager} and resolves the future so that the pusher thread can begin sending
     * messages.
     *
     * @param request information to process subscription
     * @throws StatusException if empty subscriptions informed will this exception will be thrown
     */
    private void processSubscription(StreamingPullRequest request) throws StatusException {
      if (isNull(subscriptionManager)) {
        subscriptionManager = subscriptions.get(request.getSubscription());
        if (subscriptionManager == null) {
          String message = request.getSubscription() + " is not a valid Subscription";
          LOGGER.warning(message);
          throw Status.NOT_FOUND.withDescription(message).asException();
        }
        streamId = request.getSubscription() + "-" + STREAMING_PULL_ID.getAndIncrement();
        streamAckDeadlineSecs = request.getStreamAckDeadlineSeconds();
        if (streamAckDeadlineSecs < 10 || streamAckDeadlineSecs > 600) {
          LOGGER.warning(
              String.format(
                  "%s is not a valid Stream ack deadline, reverting to default for %s (%d)s",
                  request.getStreamAckDeadlineSeconds(),
                  subscriptionManager.getSubscription().getName(),
                  subscriptionManager.getSubscription().getAckDeadlineSeconds()));
          streamAckDeadlineSecs = subscriptionManager.getSubscription().getAckDeadlineSeconds();
        }
        LOGGER.info("StreamingPull " + streamId + " initialized by client");
        streamingPullExecutorService.submit(() -> this.streamMessages(request.getSubscription()));

      } else if (!request.getSubscription().isEmpty()) {
        String message = "Subscription name can only be specified in first request";
        LOGGER.warning(message);
        throw Status.INVALID_ARGUMENT.withDescription(message).asException();
      }
    }

    /**
     * Process any ackIds contained in the request.
     *
     * @param request information to process acks
     */
    private void processAcks(StreamingPullRequest request) {
      if (request.getAckIdsCount() > 0) {
        List<String> ackIds = subscriptionManager.acknowledge(request.getAckIdsList());
        LOGGER.fine(
            "StreamingPull "
                + streamId
                + " successfully acknowledged "
                + ackIds.size()
                + " of "
                + request.getAckIdsCount()
                + " messages");
      }
    }

    /**
     * Process any modifyAckDeadlineIds contained in the request.
     *
     * @param request information to modify ack deadlines
     * @throws StatusException if counts of deadline count is diferent will throw StatusException
     */
    private void processModifyAckDeadlines(StreamingPullRequest request) throws StatusException {
      if (request.getModifyDeadlineAckIdsCount() > 0
          && request.getModifyDeadlineSecondsCount() > 0) {
        if (request.getModifyDeadlineAckIdsCount() != request.getModifyDeadlineSecondsCount()) {
          String message =
              "Request contained "
                  + request.getModifyDeadlineAckIdsCount()
                  + " modifyAckDeadlineIds but "
                  + request.getModifyDeadlineSecondsCount()
                  + " modifyDeadlineSeconds";
          LOGGER.warning(message);
          throw Status.INVALID_ARGUMENT.withDescription(message).asException();
        }

        List<String> modifiedAckIds = new ArrayList<>();
        for (int i = 0; i < request.getModifyDeadlineAckIdsCount(); i++) {
          modifiedAckIds.addAll(
              subscriptionManager.modifyAckDeadline(
                  Collections.singletonList(request.getModifyDeadlineAckIds(i)),
                  request.getModifyDeadlineSeconds(i)));
        }
        LOGGER.fine(
            "StreamingPull "
                + streamId
                + " successfully modified ack deadlines for "
                + modifiedAckIds.size()
                + " messages");
      }
    }

    /**
     * Poll in a loop until messages are available from the {@link SubscriptionManager} or the
     * stream is terminated by the client.
     */
    private void streamMessages(String subscription) {
      int pollDelay = MIN_POLL_INTERVAL;
      while (!(terminationFuture.isDone())) {
        if (responseObserver.isReady()) {
          pollDelay = MIN_POLL_INTERVAL;
          StreamingPullResponse response =
              StreamingPullResponse.newBuilder()
                  .addAllReceivedMessages(
                      buildReceivedMessageList(
                          subscriptionManager.pull(
                              MESSAGES_PER_STREAM, true, streamAckDeadlineSecs)))
                  .build();

          if (response.getReceivedMessagesCount() > 0) {
            LOGGER.fine(
                "StreamingPull "
                    + streamId
                    + " returning "
                    + response.getReceivedMessagesCount()
                    + " messages");
            responseObserver.onNext(response);
          }
        } else {
          pollDelay = Math.min(pollDelay * 2, MAX_POLL_INTERVAL);
          LOGGER.fine(
              "StreamingPull "
                  + streamId
                  + " peer is not ready, increased pollDelay to "
                  + pollDelay);
        }
        // Block on the termination future, but swallow its TimeoutException
        try {
          terminationFuture.get(pollDelay, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException ignored) {
        } catch (ExecutionException e) {
          LOGGER.severe("StreamingPull " + streamId + " encountered unrecoverable error " + e);
          responseObserver.onError(Status.fromThrowable(e).asException());
        }
      }
      if (!isNull(subscriptionManager)) {
        subscriptionManager.commitFromAcknowledgments();
      }
    }
  }
}
