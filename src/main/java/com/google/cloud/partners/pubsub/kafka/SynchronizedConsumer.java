package com.google.cloud.partners.pubsub.kafka;

import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;

/** Wrapper class around a KafkaConsumer to ensure synchronization of access across threads. */
class SynchronizedConsumer {

  private final Consumer<String, ByteBuffer> consumer;

  SynchronizedConsumer(Consumer<String, ByteBuffer> consumer) {
    this.consumer = consumer;
  }

  /** Runs the provided function provided with the Kafka Consumer client. */
  synchronized <T> T getWithConsumer(Function<Consumer<String, ByteBuffer>, T> function) {
    return function.apply(this.consumer);
  }

  /** Runs the provided consumer provided with the Kafka Consumer client. */
  synchronized void runWithConsumer(
      java.util.function.Consumer<Consumer<String, ByteBuffer>> consumer) {
    consumer.accept(this.consumer);
  }
}
