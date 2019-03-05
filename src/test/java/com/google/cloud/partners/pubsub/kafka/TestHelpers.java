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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.partners.pubsub.kafka.config.Project;
import com.google.cloud.partners.pubsub.kafka.config.PubSub;
import com.google.cloud.partners.pubsub.kafka.config.Server;
import com.google.cloud.partners.pubsub.kafka.config.Server.Kafka;
import com.google.cloud.partners.pubsub.kafka.config.Server.Security;
import com.google.cloud.partners.pubsub.kafka.config.Subscription;
import com.google.cloud.partners.pubsub.kafka.config.Topic;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

public class TestHelpers {

  public static final String PROJECT1_TOPIC1 = "projects/project-1/topics/topic-1";
  public static final String PROJECT1_TOPIC2 = "projects/project-1/topics/topic-2";
  public static final String PROJECT2_TOPIC1 = "projects/project-2/topics/topic-1";
  public static final String PROJECT2_TOPIC2 = "projects/project-2/topics/topic-2";
  public static final String PROJECT1_SUBSCRIPTION1 =
      "projects/project-1/subscriptions/subscription-1";
  public static final String PROJECT1 = "projects/project-1";
  public static final String PROJECT1_SUBSCRIPTION2 =
      "projects/project-1/subscriptions/subscription-2";
  public static final String PROJECT2_SUBSCRIPTION3 =
      "projects/project-2/subscriptions/subscription-3";
  public static final String CONFIG_FILE = "unit-test-config.json";
  public static final String PUBSUB_FILE = "unit-test-pubsub.json";

  public static final String SERVER_JSON =
      "{\n"
          + "  \"port\": 8080,\n"
          + "  \"security\": {\n"
          + "    \"certificateChainFile\": \"/path/to/server.crt\",\n"
          + "    \"privateKeyFile\": \"/path/to/server.key\"\n"
          + "  },\n"
          + "  \"kafka\": {\n"
          + "    \"bootstrapServers\": [\"server1:2192\", \"server2:2192\"],\n"
          + "    \"producerProperties\": {\n"
          + "      \"linger.ms\": \"5\",\n"
          + "      \"batch.size\": \"1000000\",\n"
          + "      \"buffer.memory\": \"32000000\"\n"
          + "    },\n"
          + "    \"producerExecutors\": 4,\n"
          + "    \"consumerProperties\": {\n"
          + "      \"max.poll.records\": \"1000\"\n"
          + "    },\n"
          + "    \"consumersPerSubscription\": 4\n"
          + "  }\n"
          + "}";

  public static final Server SERVER_CONFIG =
      Server.newBuilder()
          .setPort(8080)
          .setSecurity(
              Security.newBuilder()
                  .setCertificateChainFile("/path/to/server.crt")
                  .setPrivateKeyFile("/path/to/server.key")
                  .build())
          .setKafka(
              Kafka.newBuilder()
                  .addAllBootstrapServers(ImmutableList.of("server1:2192", "server2:2192"))
                  .putConsumerProperties("max.poll.records", "1000")
                  .putProducerProperties("linger.ms", "5")
                  .putProducerProperties("batch.size", "1000000")
                  .putProducerProperties("buffer.memory", "32000000")
                  .setProducerExecutors(4)
                  .setConsumersPerSubscription(4)
                  .build())
          .build();

  public static final String PUBSUB_JSON =
      "{\n"
          + "  \"projects\": [{\n"
          + "    \"name\": \"project-1\",\n"
          + "    \"topics\": [{\n"
          + "      \"name\": \"topic-1\",\n"
          + "      \"kafkaTopic\": \"kafka-topic-1\",\n"
          + "      \"subscriptions\": [{\n"
          + "        \"name\": \"subscription-1\",\n"
          + "        \"ackDeadlineSeconds\": 10\n"
          + "      }, {\n"
          + "        \"name\": \"subscription-2\",\n"
          + "        \"ackDeadlineSeconds\": 10\n"
          + "      }]\n"
          + "    }, {\n"
          + "      \"name\": \"topic-2\",\n"
          + "      \"kafkaTopic\": \"kafka-topic-2\",\n"
          + "      \"subscriptions\": [{\n"
          + "        \"name\": \"subscription-3\",\n"
          + "        \"ackDeadlineSeconds\": 30\n"
          + "      }, {\n"
          + "        \"name\": \"subscription-4\",\n"
          + "        \"ackDeadlineSeconds\": 45\n"
          + "      }]\n"
          + "    }]\n"
          + "  }, {\n"
          + "    \"name\": \"project-2\",\n"
          + "    \"topics\": [{\n"
          + "      \"name\": \"topic-1\",\n"
          + "      \"kafkaTopic\": \"kafka-topic-1\",\n"
          + "      \"subscriptions\": [{\n"
          + "        \"name\": \"subscription-1\",\n"
          + "        \"ackDeadlineSeconds\": 10\n"
          + "      }, {\n"
          + "        \"name\": \"subscription-2\",\n"
          + "        \"ackDeadlineSeconds\": 10\n"
          + "      }]\n"
          + "    }, {\n"
          + "      \"name\": \"topic-2\",\n"
          + "      \"kafkaTopic\": \"kafka-topic-2\",\n"
          + "      \"subscriptions\": [{\n"
          + "        \"name\": \"subscription-3\",\n"
          + "        \"ackDeadlineSeconds\": 30\n"
          + "      }]\n"
          + "    }]\n"
          + "  }]\n"
          + "}";

  public static final PubSub PUBSUB_CONFIG =
      PubSub.newBuilder()
          .addProjects(
              Project.newBuilder()
                  .setName("project-1")
                  .addTopics(
                      Topic.newBuilder()
                          .setName("topic-1")
                          .setKafkaTopic("kafka-topic-1")
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-1")
                                  .setAckDeadlineSeconds(10)
                                  .build())
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-2")
                                  .setAckDeadlineSeconds(10)
                                  .build())
                          .build())
                  .addTopics(
                      Topic.newBuilder()
                          .setName("topic-2")
                          .setKafkaTopic("kafka-topic-2")
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-3")
                                  .setAckDeadlineSeconds(30)
                                  .build())
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-4")
                                  .setAckDeadlineSeconds(45)
                                  .build())
                          .build())
                  .build())
          .addProjects(
              Project.newBuilder()
                  .setName("project-2")
                  .addTopics(
                      Topic.newBuilder()
                          .setName("topic-1")
                          .setKafkaTopic("kafka-topic-1")
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-1")
                                  .setAckDeadlineSeconds(10)
                                  .build())
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-2")
                                  .setAckDeadlineSeconds(10)
                                  .build())
                          .build())
                  .addTopics(
                      Topic.newBuilder()
                          .setName("topic-2")
                          .setKafkaTopic("kafka-topic-2")
                          .addSubscriptions(
                              Subscription.newBuilder()
                                  .setName("subscription-3")
                                  .setAckDeadlineSeconds(30)
                                  .build())
                          .build())
                  .build())
          .build();

  /** Generate a sequence of PubsubMessage objects. */
  static List<PubsubMessage> generatePubsubMessages(int howMany) {
    List<PubsubMessage> messages = new ArrayList<>();
    for (int i = 0; i < howMany; i++) {
      messages.add(
          PubsubMessage.newBuilder().setData(ByteString.copyFrom("message-" + i, UTF_8)).build());
    }
    return messages;
  }

  static List<PubsubMessage> generatePubsubMessagesWithHeader(int howMany) {
    List<PubsubMessage> messages = new ArrayList<>();
    for (int i = 0; i < howMany; i++) {
      messages.add(
          PubsubMessage.newBuilder()
              .setData(ByteString.copyFrom("message-" + i, UTF_8))
              .putAllAttributes(generateAttributes())
              .build());
    }
    return messages;
  }

  private static Map<String, String> generateAttributes() {
    Map<String, String> attributesMap = new HashMap<>();
    attributesMap.put("some-key", "some-value");
    return attributesMap;
  }

  /**
   * Generate a sequence of ConsumerRecord objects. The records will be evenly distributed amongst
   * the partitions in round-robin fashion.
   */
  static List<ConsumerRecord<String, ByteBuffer>> generateConsumerRecords(
      String topic, int partitions, int recordsPerPartition, List<Header> headers) {
    List<ConsumerRecord<String, ByteBuffer>> records = new ArrayList<>();
    int messageSeq = 0;
    long now = System.currentTimeMillis();
    for (int p = 0; p < partitions; p++) {
      for (int r = 0; r < recordsPerPartition; r++) {
        ByteBuffer value =
            ByteBuffer.wrap(String.format("message-%04d", messageSeq++).getBytes(UTF_8));
        records.add(
            new ConsumerRecord<>(
                topic,
                p,
                r,
                now,
                TimestampType.LOG_APPEND_TIME,
                (long) ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                value.capacity(),
                null,
                value,
                new RecordHeaders(headers)));
      }
    }
    return records;
  }
}
