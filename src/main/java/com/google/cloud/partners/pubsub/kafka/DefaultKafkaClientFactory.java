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

import static java.lang.Boolean.FALSE;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import java.nio.ByteBuffer;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

/** Factory implementation for building Kafka client objects based on a shared set of properties. */
@Singleton
class DefaultKafkaClientFactory implements KafkaClientFactory {

  private static final String ACKS_CONFIG_VALUE = "all";
  private static final String PRODUCER_KEY_SERIALIZER_CONFIG_VALUE =
      "org.apache.kafka.common.serialization.StringSerializer";
  private static final String PRODUCER_VALUE_SERIALIZER_CONFIG_VALUE =
      "org.apache.kafka.common.serialization.ByteBufferSerializer";
  private static final String AUTO_OFFSET_RESET_CONFIG_VALUE = "earliest";
  private static final String CONSUMER_KEY_DESERIALIZER_CONFIG_VALUE =
      "org.apache.kafka.common.serialization.StringDeserializer";
  private static final String CONSUMER_VALUE_DESERIALIZER_CONFIG_VALUE =
      "org.apache.kafka.common.serialization.ByteBufferDeserializer";
  private static final int MAX_BLOCK_MS_VALUE = 2000;

  private final ConfigurationManager configurationManager;
  private final String bootstrapServersJoined;

  /**
   * Create a new KafkaClientFactoryImpl which will be used to provide instances of Kafka client
   * objects to produce or consume records to/from topics.
   */
  @Inject
  DefaultKafkaClientFactory(ConfigurationManager configurationManager) {
    this.configurationManager = configurationManager;
    bootstrapServersJoined =
        String.join(",", configurationManager.getServer().getKafka().getBootstrapServersList());
  }

  /**
   * Builds and returns a new KafkaConsumer object using the Consumer group.id {@code subscription}
   * to define a logical subscriber.
   */
  @Override
  public Consumer<String, ByteBuffer> createConsumer(String subscription) {
    Properties properties = new Properties();
    properties.putAll(configurationManager.getServer().getKafka().getConsumerPropertiesMap());
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersJoined);
    properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, FALSE.toString());
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG_VALUE);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, CONSUMER_KEY_DESERIALIZER_CONFIG_VALUE);
    properties.setProperty(
        VALUE_DESERIALIZER_CLASS_CONFIG, CONSUMER_VALUE_DESERIALIZER_CONFIG_VALUE);
    properties.setProperty(GROUP_ID_CONFIG, subscription);
    return new KafkaConsumer<>(properties);
  }

  /** Builds and returns a new KafkaProducer object. */
  @Override
  public Producer<String, ByteBuffer> createProducer() {
    Properties properties = new Properties();
    properties.putAll(configurationManager.getServer().getKafka().getProducerPropertiesMap());
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersJoined);
    properties.setProperty(ACKS_CONFIG, ACKS_CONFIG_VALUE);
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, PRODUCER_KEY_SERIALIZER_CONFIG_VALUE);
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, PRODUCER_VALUE_SERIALIZER_CONFIG_VALUE);
    properties.setProperty(MAX_BLOCK_MS_CONFIG, String.valueOf(MAX_BLOCK_MS_VALUE));
    return new KafkaProducer<>(properties);
  }
}
