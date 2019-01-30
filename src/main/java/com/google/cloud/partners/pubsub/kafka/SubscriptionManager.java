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

import com.google.common.util.concurrent.AbstractScheduledService;
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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * message consumer. Based on the configuration settings, a number of KafkaConsumers will be created
 * and assigned to the partitions of the topic indicated by the {@link Subscription} provided.
 *
 * <p>KafkaConsumer objects are not threadsafe, so care must be taken to ensure that appropriate
 * synchronization controls are in place for any methods that need to interact with a consumer. We
 * synchronize on each KafkaConsumer when using their poll() or commitSync() methods.
 */
class SubscriptionManager extends AbstractScheduledService {

  private static final Logger LOGGER = Logger.getLogger(SubscriptionManager.class.getName());
  private static final int COMMIT_DELAY = 2; // 2 seconds
  private static final long POLL_TIMEOUT = 5000; // 5 seconds

  private final String kafkaTopicName;
  private final KafkaClientFactory kafkaClientFactory;
  private final Clock clock;

  // TODO: Recreate consumers if any lose connection with brokers
  private final List<SynchronizedConsumer> kafkaConsumers;
  private final Map<TopicPartition, OffsetAndMetadata> committedOffsets;
  private final AtomicInteger nextConsumerIndex, queueSizeBytes;
  private final Queue<ConsumerRecord<String, ByteBuffer>> buffer;
  private final Map<String, OutstandingMessage> outstandingMessages;
  private final int consumerExecutors;
  private final Subscription subscription;
  private String hostName;

  SubscriptionManager(
      Subscription subscription,
      KafkaClientFactory kafkaClientFactory,
      Clock clock,
      int consumerExecutors) {
    this.consumerExecutors = consumerExecutors;
    this.subscription = subscription;
    this.kafkaClientFactory = kafkaClientFactory;
    this.clock = clock;

    kafkaTopicName = subscription.getLabelsOrDefault(KAFKA_TOPIC, subscription.getTopic());
    committedOffsets = new HashMap<>();

    buffer = new ConcurrentLinkedQueue<>();
    outstandingMessages = new ConcurrentHashMap<>();
    nextConsumerIndex = new AtomicInteger();
    queueSizeBytes = new AtomicInteger();
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      hostName = "<Unknown Host>";
    }

    kafkaConsumers = new ArrayList<>();
  }

  public Subscription getSubscription() {
    return subscription;
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
    return ackIds
        .stream()
        .map(this::getOutstandingIfNotExpired)
        .filter(Optional::isPresent)
        .map(
            o -> {
              OutstandingMessage om = o.get();
              om.setAcknowledged(true);
              return om.getMessageId();
            })
        .collect(Collectors.toList());
  }

  /**
   * Modify each provided and unacknowledged Message by setting its ackExpiration property to
   * ackDeadlineSecs from now.
   *
   * @return List of ackIds that were successfully modified before their expiration
   */
  public List<String> modifyAckDeadline(List<String> ackIds, int ackDeadlineSecs) {
    return ackIds
        .stream()
        .map(this::getOutstandingIfNotExpired)
        .filter(Optional::isPresent)
        .map(
            o -> {
              OutstandingMessage om = o.get();
              om.addSecondsToDeadline(ackDeadlineSecs);
              return om.getMessageId();
            })
        .collect(Collectors.toList());
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

  @Override
  protected void runOneIteration() {
    commitFromAcknowledgments();
  }

  /**
   * Initializes and returns a List of Consumers that are manually assigned to specific
   * TopicPartitions. We choose to use manual assignment to avoid the timeout, blocking, and
   * heartbeats required when using dynamic subscriptions.
   *
   * @return List of Consumers assigned to partitions from the topic
   */
  @Override
  protected void startUp() {
    // Create first consumer and determine # partitions in topic
    Consumer<String, ByteBuffer> first = kafkaClientFactory.createConsumer(subscription.getName());
    List<PartitionInfo> partitionInfo = first.partitionsFor(kafkaTopicName);

    int totalConsumers = Math.min(partitionInfo.size(), consumerExecutors);
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
        LOGGER.fine(
            String.format(
                "%s assigned KafkaConsumer %d to %s:%d (End: %d, Committed %d)",
                subscription.getName(),
                consumerIndex,
                tp.topic(),
                tp.partition(),
                endOffsets.get(tp),
                Optional.ofNullable(consumer.committed(tp))
                    .map(OffsetAndMetadata::offset)
                    .orElse(0L)));
      }
      kafkaConsumers.add(new SynchronizedConsumer(consumer));
    }
  }

  @Override
  protected void shutDown() {
    commitFromAcknowledgments();
    int closed = 0;
    for (SynchronizedConsumer consumer : kafkaConsumers) {
      consumer.runWithConsumer(Consumer::close);
      closed++;
    }
    LOGGER.info("Closed " + closed + " KafkaConsumers for " + subscription.getName());
  }

  @Override
  protected Scheduler scheduler() {
    return AbstractScheduledService.Scheduler.newFixedDelaySchedule(
        COMMIT_DELAY, COMMIT_DELAY, TimeUnit.SECONDS);
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
  private synchronized void commitFromAcknowledgments() {
    Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
    String commitMetadata = "Committed at " + clock.instant().toEpochMilli() + " by " + hostName;
    AckInfo[] ackInfos = getAckInfoByPartition();

    // Determine new offsets for each TopicPartition
    for (TopicPartition tp : committedOffsets.keySet()) {
      AckInfo ackInfo = ackInfos[tp.partition()];
      // Check unacked set first
      if (ackInfo.getSmallestUnacknowledged().isPresent()) {
        commits.put(
            tp, new OffsetAndMetadata(ackInfo.getSmallestUnacknowledged().get(), commitMetadata));
      } else if (ackInfo.getLargestAcknowledged().isPresent()) {
        commits.put(
            tp, new OffsetAndMetadata(ackInfo.getLargestAcknowledged().get() + 1, commitMetadata));
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
        try {
          SynchronizedConsumer consumer = kafkaConsumers.get(i);
          consumer.runWithConsumer(c -> c.commitSync(consumerCommits));
          for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumerCommits.entrySet()) {
            LOGGER.fine(
                String.format(
                    "%s KafkaConsumer %d seeking to newly committed offset %d of %s:%d",
                    subscription.getName(),
                    i,
                    entry.getValue().offset(),
                    entry.getKey().topic(),
                    entry.getKey().partition()));
            consumer.runWithConsumer(c -> c.seek(entry.getKey(), entry.getValue().offset()));
            purgeAcknowledged(entry.getKey().partition(), entry.getValue().offset());
          }
          committedOffsets.putAll(consumerCommits);
        } catch (KafkaException e) {
          // TODO: Handle this which might include creating a new consumer
          LOGGER.log(Level.WARNING, "Unexpected KafkaException during commit", e);
        }
      }
    }
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
    try {
      polled = kafkaConsumers.get(consumerIndex).getWithConsumer(c -> c.poll(pollTimeout));
    } catch (KafkaException e) {
      // TODO: Handle this which might include creating a new consumer
      LOGGER.log(Level.WARNING, "Unexpected KafkaException during poll", e);
    }

    if (polled != null) {
      for (ConsumerRecord<String, ByteBuffer> record : polled.records(kafkaTopicName)) {
        buffer.add(record);
        newMessages++;
        queueSizeBytes.addAndGet(record.serializedValueSize());
      }
      LOGGER.fine(
          String.format(
              "poll(%d) from consumer %d returned %d messages (buffer=%d, bufferBytes=%d)",
              pollTimeout, consumerIndex, newMessages, buffer.size(), queueSizeBytes.get()));
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
        String.format(
            "Purged %d committed messages from partition %d (%d outstanding)",
            purgeList.size(), partition, outstandingMessages.size()));
  }

  private Optional<OutstandingMessage> getOutstandingIfNotExpired(String messageId) {
    Instant now = clock.instant();
    return Optional.ofNullable(outstandingMessages.get(messageId)).filter(om -> !om.isExpired(now));
  }

  // Determine the smallest unack'ed and largest ack'ed for each partition
  private AckInfo[] getAckInfoByPartition() {
    AckInfo[] ackInfos = new AckInfo[committedOffsets.size()];
    for (int i = 0; i < ackInfos.length; i++) {
      ackInfos[i] = new AckInfo();
    }
    for (OutstandingMessage m : outstandingMessages.values()) {
      long offset = m.getOffset();
      AckInfo ackInfo = ackInfos[m.getPartition()];
      if (!m.isAcknowledged()
          && offset < ackInfo.getSmallestUnacknowledged().orElse(Long.MAX_VALUE)) {
        ackInfo.setSmallestUnacknowledged(offset);
      } else if (m.isAcknowledged() && offset > ackInfo.getLargestAcknowledged().orElse(0L)) {
        ackInfo.setLargestAcknowledged(offset);
      }
    }
    return ackInfos;
  }

  // Container class when calculating offsets to commit from acknowledgements
  private static class AckInfo {
    private Long smallestUnacknowledged;
    private Long largestAcknowledged;

    Optional<Long> getSmallestUnacknowledged() {
      return Optional.ofNullable(smallestUnacknowledged);
    }

    void setSmallestUnacknowledged(Long smallestUnacknowledged) {
      this.smallestUnacknowledged = smallestUnacknowledged;
    }

    Optional<Long> getLargestAcknowledged() {
      return Optional.ofNullable(largestAcknowledged);
    }

    void setLargestAcknowledged(Long largestAcknowledged) {
      this.largestAcknowledged = largestAcknowledged;
    }
  }
}
