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

import com.google.cloud.partners.pubsub.kafka.properties.ConsumerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.SubscriptionProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

/**
 * A {@code SubscriptionManager} is responsible for handling the communication with Kafka as a
 * message consumer. Based on the {@link ConsumerProperties#getExecutors()} setting, a number of
 * KafkaConsumers will be created and assigned to the partitions of the topic indicated by the
 * {@link SubscriptionProperties} provided.
 *
 * <p>KafkaConsumer objects are not threadsafe, so care must be taken to ensure that appropriate
 * synchronization controls are in place for any methods that need to interact with a consumer. We
 * synchronize on each KafkaConsumer when using their poll() or commitSync() methods.
 */
class SubscriptionManager {

  private static final Logger LOGGER = Logger.getLogger(SubscriptionManager.class.getName());
  private static final int COMMIT_DELAY = 5; // 5 seconds
  private static final long POLL_TIMEOUT = 5000; // 5 seconds

  private final String kafkaTopicName;
  private final KafkaClientFactory kafkaClientFactory;
  private final Clock clock;
  private final ScheduledExecutorService commitExecutorService;

  // TODO: Recreate consumers if any lose connection with brokers
  private final List<Consumer<String, ByteBuffer>> kafkaConsumers;
  private final Map<TopicPartition, OffsetAndMetadata> committedOffsets;
  private final AtomicInteger nextConsumerIndex, queueSizeBytes;
  private final Queue<ConsumerRecord<String, ByteBuffer>> buffer;
  private final Map<String, OutstandingMessage> outstandingMessages;
  private final AtomicReference<ScheduledFuture<?>> commitFuture;
  private final int consumerExecutors;
  private final Subscription subscription;
  private String hostName;
  private boolean shutdown;

  SubscriptionManager(
      Subscription subscription,
      KafkaClientFactory kafkaClientFactory,
      Clock clock,
      ScheduledExecutorService commitExecutorService) {
    this.consumerExecutors =
        Configuration.getApplicationProperties()
            .getKafkaProperties()
            .getConsumerProperties()
            .getExecutors();
    this.subscription = subscription;
    this.kafkaClientFactory = kafkaClientFactory;
    this.clock = clock;
    this.commitExecutorService = commitExecutorService;

    kafkaTopicName = subscription.getLabelsOrDefault(KAFKA_TOPIC, subscription.getTopic());
    committedOffsets = new HashMap<>();
    kafkaConsumers = initializeConsumers();

    buffer = new ConcurrentLinkedQueue<>();
    outstandingMessages = new ConcurrentHashMap<>();
    commitFuture = new AtomicReference<>();
    nextConsumerIndex = new AtomicInteger();
    queueSizeBytes = new AtomicInteger();
    shutdown = false;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      hostName = "<Unknown Host>";
    }
  }

  public Subscription getSubscription() {
    return subscription;
  }

  /** Shutdown hook should close all Consumers */
  public void shutdown() {
    commitFromAcknowledgments();
    shutdown = true;
    int closed = 0;
    for (Consumer<String, ByteBuffer> kafkaConsumer : kafkaConsumers) {
      synchronized (kafkaConsumer) {
        kafkaConsumer.close();
      }
      closed++;
    }
    LOGGER.info("Closed " + closed + " KafkaConsumers");
  }

  /**
   * Returns up to {@code maxMessages}. If {@code returnImmediately} is true, the method will
   * respond by returning the contents of the in-memory queued messages, or the messages available
   * from Kafka without incurring any delay.
   *
   * @return List of PubsubMessage objects.
   */
  public List<PubsubMessage> pull(int maxMessages, boolean returnImmediately) {
    return pull(maxMessages, returnImmediately, subscription.getAckDeadlineSeconds());
  }

  /**
   * Returns up to {@code maxMessages} with their acknowledgement deadline set to {@code
   * ackDeadlineSecs}. If {@code returnImmediately} is true, the method will respond by returning
   * the contents of the in-memory queued messages, or the messages available from Kafka without
   * incurring any delay.
   *
   * @return List of PubsubMessage objects.
   */
  public List<PubsubMessage> pull(int maxMessages, boolean returnImmediately, int ackDeadlineSecs) {
    List<PubsubMessage> response = new ArrayList<>();
    fillFromBuffer(response, maxMessages);
    if ((!returnImmediately && response.size() < maxMessages) || response.isEmpty()) {
      long timeout = returnImmediately ? 0 : POLL_TIMEOUT;
      consumeFromTopic(timeout);
      fillFromBuffer(response, maxMessages);
    }

    // Add each message to the outstanding map
    Instant now = clock.instant();
    response.forEach(
        m -> {
          OutstandingMessage om =
              OutstandingMessage.newBuilder()
                  .setPulledAt(now)
                  .setExpiresAt(now.plusSeconds(ackDeadlineSecs))
                  .setMessageId(m.getMessageId())
                  .build();
          outstandingMessages.put(m.getMessageId(), om);
        });

    return response;
  }

  /**
   * Evaluate each ackId against the outstanding set setting its acknowledged bit if the ack was
   * received prior to its expiration. This method has no effect on Kafka's offsets as commits are
   * issued in a separate worker thread.
   *
   * @param ackIds List of ackIds to be acknowledged
   * @return List of ackIds that were successfully acknowledged before their expiration
   */
  public List<String> acknowledge(List<String> ackIds) {
    List<String> response = new ArrayList<>();
    Instant now = clock.instant();
    ackIds
        .stream()
        .filter(outstandingMessages::containsKey)
        .map(outstandingMessages::get)
        .forEach(
            om -> {
              if (!om.isExpired(now)) {
                om.setAcknowledged(true);
                response.add(om.getMessageId());
              } else {
                LOGGER.fine(
                    "Message "
                        + om.getMessageId()
                        + " expired at "
                        + om.getExpiresAt().toEpochMilli());
              }
            });

    // Schedule a commit operation if commitFuture is not already present
    if (!response.isEmpty() && commitFuture.get() == null) {
      commitFuture.set(
          commitExecutorService.schedule(
              () -> {
                commitFromAcknowledgments();
                commitFuture.set(null);
              },
              COMMIT_DELAY,
              TimeUnit.SECONDS));
    }
    return response;
  }

  /**
   * Process all acknowledged messages by setting the subscribed topic's partition offsets to the
   * largest acknowledged offset + 1. The committed offset for each partition is determined by
   * finding the smallest unacknowledged offset + 1. If all outstanding messages are acknowledged,
   * the largest offset + 1 will be used. This method is synchronized so that the effect of
   * evaluating the committed state is consistent between threads and we avoid unnecessary commit
   * operations.
   *
   * @return Map of TopicPartitions and the offsets that are being committed
   */
  public synchronized Map<TopicPartition, OffsetAndMetadata> commitFromAcknowledgments() {
    Map<Integer, Long> smallestUnacked =
        outstandingMessages
            .values()
            .stream()
            .filter(m -> !m.isAcknowledged())
            .collect(
                Collectors.toMap(
                    OutstandingMessage::getPartition,
                    OutstandingMessage::getOffset,
                    (o1, o2) -> o1.compareTo(o2) <= 0 ? o1 : o2));
    Map<Integer, Long> largestAcked =
        outstandingMessages
            .values()
            .stream()
            .filter(OutstandingMessage::isAcknowledged)
            .collect(
                Collectors.toMap(
                    OutstandingMessage::getPartition,
                    OutstandingMessage::getOffset,
                    (o1, o2) -> o1.compareTo(o2) >= 0 ? o1 : o2));

    Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
    if (shutdown) {
      LOGGER.warning("commitFromAcknowledgments called after shutdown will return immediately");
      return commits;
    }

    String commitMetadata = "Committed at " + clock.instant().toEpochMilli() + " by " + hostName;
    // Determine new offsets for each TopicPartition
    for (TopicPartition tp : committedOffsets.keySet()) {
      // Check unacked set first
      Long ackOffset = smallestUnacked.get(tp.partition());
      if (ackOffset != null) {
        commits.put(tp, new OffsetAndMetadata(ackOffset, commitMetadata));
      } else {
        ackOffset = largestAcked.get(tp.partition());
        if (ackOffset != null) {
          commits.put(tp, new OffsetAndMetadata(ackOffset + 1, commitMetadata));
        }
      }
    }

    // Determine the commits to issue for each consumer
    for (int i = 0; i < kafkaConsumers.size(); i++) {
      Map<TopicPartition, OffsetAndMetadata> consumerCommits = new HashMap<>();
      boolean shouldCommit = false;
      for (int p = i; p < committedOffsets.keySet().size(); p += kafkaConsumers.size()) {
        TopicPartition tp = new TopicPartition(kafkaTopicName, p);
        OffsetAndMetadata current = committedOffsets.get(tp);
        OffsetAndMetadata newOffset = commits.get(tp);
        if (newOffset != null && (current == null || newOffset.offset() > current.offset())) {
          shouldCommit = true;
          consumerCommits.put(tp, newOffset);
        }
      }
      if (shouldCommit) {
        synchronized (kafkaConsumers.get(i)) {
          LOGGER.fine(
              "Consumer "
                  + i
                  + " committing "
                  + consumerCommits
                      .entrySet()
                      .stream()
                      .sorted(
                          (es1, es2) ->
                              es1.getKey().partition() < es2.getKey().partition() ? -1 : 1)
                      .map(es -> "P" + es.getKey().partition() + ":" + es.getValue().offset())
                      .collect(Collectors.joining(", ")));
          try {
            kafkaConsumers.get(i).commitSync(consumerCommits);
            Consumer<String, ByteBuffer> finalConsumer = kafkaConsumers.get(i);
            consumerCommits.forEach((t, o) -> finalConsumer.seek(t, o.offset()));
            consumerCommits.forEach(
                (key, value) -> purgeAcknowledged(key.partition(), value.offset()));
            committedOffsets.putAll(consumerCommits);
          } catch (KafkaException e) {
            // TODO: Handle this which might include creating a new consumer
            LOGGER.log(Level.WARNING, "Unexpected KafkaException during commit", e);
          }
        }
      }
    }
    return commits;
  }

  /**
   * Modify each provided and unacknowledged Message by setting its ackExpiration property to
   * ackDeadlineSecs from now.
   *
   * @return List of ackIds that were successfully modified before their expiration
   */
  public List<String> modifyAckDeadline(List<String> ackIds, int ackDeadlineSecs) {
    List<String> response = new ArrayList<>();
    Instant now = clock.instant();
    ackIds
        .stream()
        .filter(outstandingMessages::containsKey)
        .map(outstandingMessages::get)
        .forEach(
            om -> {
              if (!om.isExpired(now)) {
                om.addSecondsToDeadline(ackDeadlineSecs);
                response.add(om.getMessageId());
              } else {
                LOGGER.fine(
                    "Message "
                        + om.getMessageId()
                        + " expired at "
                        + om.getExpiresAt().toEpochMilli());
              }
            });
    return response;
  }

  @Override
  public String toString() {
    return "SubscriptionManager{"
        + "name="
        + subscription.getName()
        + ", topic="
        + subscription.getTopic()
        + ", kafkaTopic="
        + kafkaTopicName
        + ", buffer="
        + buffer.size()
        + ", bufferBytes="
        + queueSizeBytes.get()
        + ", outstandingMessages="
        + outstandingMessages.size()
        + '}';
  }

  /**
   * Fetches the next available {@link org.apache.kafka.clients.consumer.Consumer} from the pool and
   * issues a poll request to the broker with the specified timeout.
   *
   * @param pollTimeout information of timeout
   */
  private void consumeFromTopic(long pollTimeout) {
    int consumerIndex = nextConsumerIndex.getAndUpdate((value) -> ++value % kafkaConsumers.size());
    int newMessages = 0;
    ConsumerRecords<String, ByteBuffer> polled = null;
    synchronized (kafkaConsumers.get(consumerIndex)) {
      try {
        polled = kafkaConsumers.get(consumerIndex).poll(pollTimeout);
      } catch (KafkaException e) {
        // TODO: Handle this which might include creating a new consumer
        LOGGER.log(Level.WARNING, "Unexpected KafkaException during poll", e);
      }
    }

    if (polled != null) {
      for (ConsumerRecord<String, ByteBuffer> record : polled.records(kafkaTopicName)) {
        buffer.add(record);
        newMessages++;
        queueSizeBytes.addAndGet(record.serializedValueSize());
      }
      LOGGER.fine(
          "poll("
              + pollTimeout
              + ") from consumer "
              + consumerIndex
              + " returned "
              + newMessages
              + " buffer="
              + buffer.size()
              + ", bufferBytes="
              + queueSizeBytes.get());
    }
  }

  /**
   * Fills the {@code returnedMessages} List with up to {@code maxMessages} Message objects by
   * retrieving ConsumerRecords from the head of the buffered queue.
   */
  private void fillFromBuffer(List<PubsubMessage> returnedMessages, int maxMessages) {
    ConsumerRecord<String, ByteBuffer> record;
    int dequeued = 0;
    String messageId;
    while (returnedMessages.size() < maxMessages && !buffer.isEmpty()) {
      try {
        record = buffer.remove();
        dequeued++;
        messageId = record.partition() + "-" + record.offset();
        queueSizeBytes.addAndGet(-record.serializedValueSize());
        returnedMessages.add(
            PubsubMessage.newBuilder()
                .putAllAttributes(buildAttributesMap(record.headers()))
                .setData(ByteString.copyFrom(record.value()))
                .setMessageId(messageId)
                .setPublishTime(
                    Timestamp.newBuilder()
                        .setSeconds(record.timestamp() / 1000)
                        .setNanos((int) ((record.timestamp() % 1000) * 1000000))
                        .build())
                .build());
      } catch (NoSuchElementException e) {
        break;
      }
    }
    LOGGER.fine("Dequeued " + dequeued + " messages from buffer");
  }

  private Map<String, String> buildAttributesMap(Headers headers) {
    if (Objects.isNull(headers)) {
      return null;
    }
    Map<String, String> attributesMap = new HashMap<>();
    headers.forEach(header -> attributesMap.put(header.key(), new String(header.value())));
    return attributesMap;
  }

  /**
   * Purges outstanding messages from memory from the specified {@code partition} whose offsets are
   * less than {@code maxOffset}.
   */
  private void purgeAcknowledged(int partition, long maxOffset) {
    List<String> purgeList =
        outstandingMessages
            .values()
            .stream()
            .filter(m -> m.getPartition() == partition && m.getOffset() < maxOffset)
            .map(OutstandingMessage::getMessageId)
            .collect(Collectors.toList());
    purgeList.forEach(outstandingMessages::remove);

    LOGGER.fine(
        "Purged "
            + purgeList.size()
            + " committed messages from partition "
            + partition
            + " ("
            + outstandingMessages.size()
            + " outstanding)");
  }

  /**
   * Initializes and returns a List of Consumers that are manually assigned to specific
   * TopicPartitions. We choose to use manual assignment to avoid the timeout, blocking, and
   * heartbeats required when using dynamic subscriptions.
   *
   * @return List of Consumers assigned to partitions from the topic
   */
  private List<Consumer<String, ByteBuffer>> initializeConsumers() {
    // Create first consumer and determine # partitions in topic
    Consumer<String, ByteBuffer> first = kafkaClientFactory.createConsumer(subscription.getName());
    List<PartitionInfo> partitionInfo = first.partitionsFor(kafkaTopicName);

    int totalConsumers = Math.min(partitionInfo.size(), consumerExecutors);
    List<Consumer<String, ByteBuffer>> consumers = new ArrayList<>(totalConsumers);
    for (int i = 0; i < totalConsumers; i++) {
      Consumer<String, ByteBuffer> consumer =
          i == 0 ? first : kafkaClientFactory.createConsumer(subscription.getName());
      int consumerIndex = i;
      Set<TopicPartition> partitionSet =
          partitionInfo
              .stream()
              .filter(p -> p.partition() % totalConsumers == consumerIndex)
              .map(p -> new TopicPartition(kafkaTopicName, p.partition()))
              .collect(Collectors.toSet());
      consumer.assign(partitionSet);
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitionSet);
      for (TopicPartition tp : partitionSet) {
        committedOffsets.put(tp, consumer.committed(tp));
        LOGGER.info(
            "Assigned KafkaConsumer "
                + consumerIndex
                + " to "
                + " Partition: "
                + tp.partition()
                + " End: "
                + endOffsets.get(tp)
                + " Committed: "
                + consumer.committed(tp));
      }
      consumers.add(consumer);
    }
    return consumers;
  }
}
