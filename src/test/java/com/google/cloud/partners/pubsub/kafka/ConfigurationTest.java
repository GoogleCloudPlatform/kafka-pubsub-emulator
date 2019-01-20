package com.google.cloud.partners.pubsub.kafka;

import static org.junit.Assert.assertEquals;

import com.google.cloud.partners.pubsub.kafka.config.Configuration;
import com.google.cloud.partners.pubsub.kafka.config.Kafka;
import com.google.cloud.partners.pubsub.kafka.config.Project;
import com.google.cloud.partners.pubsub.kafka.config.PubSub;
import com.google.cloud.partners.pubsub.kafka.config.Server;
import com.google.cloud.partners.pubsub.kafka.config.Server.Security;
import com.google.cloud.partners.pubsub.kafka.config.Topic;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.google.protobuf.util.JsonFormat;
import org.junit.Test;

public class ConfigurationTest {

  @Test
  public void configurationToProtoText() throws InvalidProtocolBufferException {
    Configuration configuration = Configuration.newBuilder()
        .setServer(Server.newBuilder()
            .setPort(8080)
            .setSecurity(Security.newBuilder()
                .setCertificateChainFile("/path/to/server.crt")
                .setPrivateKeyFile("/path/to/server.key")
                .build())
            .build())
        .setKafka(Kafka.newBuilder()
            .addAllBootstrapServers(ImmutableList.of("server1:2192", "server2:2192"))
            .putProducerProperties("max.poll.records", "1000")
            .putConsumerProperties("linger.ms", "5")
            .putConsumerProperties("batch.size", "1000000")
            .putConsumerProperties("buffer.memory", "32000000")
            .setProducerExecutors(4)
            .setConsumersPerSubscription(4)
            .build())
        .setPubsub(PubSub.newBuilder()
            .addProjects(Project.newBuilder()
                .setName("project-1")
                .addTopics(Topic.newBuilder()
                    .setName("topic-1")
                    .setKafkaTopic("kafka-topic-1")
                    .addAllSubscriptions(ImmutableList.of("subscription-1", "subscription-2"))
                    .build())
                .addTopics(Topic.newBuilder()
                    .setName("topic-2")
                    .setKafkaTopic("kafka-topic-2")
                    .addAllSubscriptions(ImmutableList.of("subscription-1", "subscription-2"))
                    .build())
                .build())
            .addProjects(Project.newBuilder()
                .setName("project-2")
                .addTopics(Topic.newBuilder()
                    .setName("topic-1")
                    .setKafkaTopic("kafka-topic-1")
                    .addAllSubscriptions(ImmutableList.of("subscription-1", "subscription-2"))
                    .build())
                .addTopics(Topic.newBuilder()
                    .setName("topic-2")
                    .setKafkaTopic("kafka-topic-2")
                    .addAllSubscriptions(ImmutableList.of("subscription-1", "subscription-2"))
                    .build())
                .build())
            .build())
        .build();
    System.out.println("<<<<<< JSON >>>>");
    System.out.println(JsonFormat.printer().print(configuration));
    System.out.println("<<<<<< TEXT >>>>");
    System.out.println(TextFormat.printToString(configuration));
  }

  @Test
  public void configurationFromProtoText() throws ParseException, InvalidProtocolBufferException {
    String text = "server {\n"
        + "  port: 8080\n"
        + "  security {\n"
        + "    certificate_chain_file: \"/path/to/server.crt\"\n"
        + "    private_key_file: \"/path/to/server.key\"\n"
        + "  }\n"
        + "}\n"
        + "kafka {\n"
        + "  bootstrap_servers: \"server1:2192\"\n"
        + "  bootstrap_servers: \"server2:2192\"\n"
        + "  producer_properties {\n"
        + "    key: \"max.poll.records\"\n"
        + "    value: \"1000\"\n"
        + "  }\n"
        + "  producer_executors: 4\n"
        + "  consumer_properties {\n"
        + "    key: \"linger.ms\"\n"
        + "    value: \"5\"\n"
        + "  }\n"
        + "  consumer_properties {\n"
        + "    key: \"batch.size\"\n"
        + "    value: \"1000000\"\n"
        + "  }\n"
        + "  consumer_properties {\n"
        + "    key: \"buffer.memory\"\n"
        + "    value: \"32000000\"\n"
        + "  }\n"
        + "  consumers_per_subscription: 4\n"
        + "}\n"
        + "pubsub {\n"
        + "  projects {\n"
        + "    name: \"project-1\"\n"
        + "    topics {\n"
        + "      name: \"topic-1\"\n"
        + "      kafka_topic: \"kafka-topic-1\"\n"
        + "      subscriptions: \"subscription-1\"\n"
        + "      subscriptions: \"subscription-2\"\n"
        + "    }\n"
        + "    topics {\n"
        + "      name: \"topic-2\"\n"
        + "      kafka_topic: \"kafka-topic-2\"\n"
        + "      subscriptions: \"subscription-1\"\n"
        + "      subscriptions: \"subscription-2\"\n"
        + "    }\n"
        + "  }\n"
        + "  projects {\n"
        + "    name: \"project-2\"\n"
        + "    topics {\n"
        + "      name: \"topic-1\"\n"
        + "      kafka_topic: \"kafka-topic-1\"\n"
        + "      subscriptions: \"subscription-1\"\n"
        + "      subscriptions: \"subscription-2\"\n"
        + "    }\n"
        + "    topics {\n"
        + "      name: \"topic-2\"\n"
        + "      kafka_topic: \"kafka-topic-2\"\n"
        + "      subscriptions: \"subscription-1\"\n"
        + "      subscriptions: \"subscription-2\"\n"
        + "    }\n"
        + "  }\n"
        + "}";
    String json = "{\n"
        + "  \"server\": {\n"
        + "    \"port\": 8080,\n"
        + "    \"security\": {\n"
        + "      \"certificateChainFile\": \"/path/to/server.crt\",\n"
        + "      \"privateKeyFile\": \"/path/to/server.key\"\n"
        + "    }\n"
        + "  },\n"
        + "  \"kafka\": {\n"
        + "    \"bootstrapServers\": [\"server1:2192\", \"server2:2192\"],\n"
        + "    \"producerProperties\": {\n"
        + "      \"max.poll.records\": \"1000\"\n"
        + "    },\n"
        + "    \"producerExecutors\": 4,\n"
        + "    \"consumerProperties\": {\n"
        + "      \"linger.ms\": \"5\",\n"
        + "      \"batch.size\": \"1000000\",\n"
        + "      \"buffer.memory\": \"32000000\"\n"
        + "    },\n"
        + "    \"consumersPerSubscription\": 4\n"
        + "  },\n"
        + "  \"pubsub\": {\n"
        + "    \"projects\": [{\n"
        + "      \"name\": \"project-1\",\n"
        + "      \"topics\": [{\n"
        + "        \"name\": \"topic-1\",\n"
        + "        \"kafkaTopic\": \"kafka-topic-1\",\n"
        + "        \"subscriptions\": [\"subscription-1\", \"subscription-2\"]\n"
        + "      }, {\n"
        + "        \"name\": \"topic-2\",\n"
        + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
        + "        \"subscriptions\": [\"subscription-1\", \"subscription-2\"]\n"
        + "      }]\n"
        + "    }, {\n"
        + "      \"name\": \"project-2\",\n"
        + "      \"topics\": [{\n"
        + "        \"name\": \"topic-1\",\n"
        + "        \"kafkaTopic\": \"kafka-topic-1\",\n"
        + "        \"subscriptions\": [\"subscription-1\", \"subscription-2\"]\n"
        + "      }, {\n"
        + "        \"name\": \"topic-2\",\n"
        + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
        + "        \"subscriptions\": [\"subscription-1\", \"subscription-2\"]\n"
        + "      }]\n"
        + "    }]\n"
        + "  }\n"
        + "}";


    Configuration.Builder builder = Configuration.newBuilder();
    TextFormat.merge(text, builder);
    Configuration fromText = builder.build();

    builder = Configuration.newBuilder();
    JsonFormat.parser().merge(json, builder);
    Configuration fromJson = builder.build();

    assertEquals(fromText, fromJson);
  }

}
