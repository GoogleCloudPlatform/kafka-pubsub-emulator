package com.google.cloud.partners.pubsub.kafka.config;

import static com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.KAFKA_TOPIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationAlreadyExistsException;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository.ConfigurationNotFoundException;
import com.google.pubsub.v1.Subscription;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class FileConfigurationRepositoryTest {

  private static final String CONFIG =
      "{\n"
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
          + "        \"subscriptions\": [{\n"
          + "          \"name\": \"subscription-1\",\n"
          + "          \"ackDeadlineSeconds\": 10\n"
          + "        }, {\n"
          + "          \"name\": \"subscription-2\",\n"
          + "          \"ackDeadlineSeconds\": 10\n"
          + "        }]\n"
          + "      }, {\n"
          + "        \"name\": \"topic-2\",\n"
          + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
          + "        \"subscriptions\": [{\n"
          + "          \"name\": \"subscription-3\",\n"
          + "          \"ackDeadlineSeconds\": 30\n"
          + "        }, {\n"
          + "          \"name\": \"subscription-4\",\n"
          + "          \"ackDeadlineSeconds\": 45\n"
          + "        }]\n"
          + "      }]\n"
          + "    }, {\n"
          + "      \"name\": \"project-2\",\n"
          + "      \"topics\": [{\n"
          + "        \"name\": \"topic-1\",\n"
          + "        \"kafkaTopic\": \"kafka-topic-1\",\n"
          + "        \"subscriptions\": [{\n"
          + "          \"name\": \"subscription-1\",\n"
          + "          \"ackDeadlineSeconds\": 10\n"
          + "        }, {\n"
          + "          \"name\": \"subscription-2\",\n"
          + "          \"ackDeadlineSeconds\": 10\n"
          + "        }]\n"
          + "      }, {\n"
          + "        \"name\": \"topic-2\",\n"
          + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
          + "        \"subscriptions\": [{\n"
          + "          \"name\": \"subscription-3\",\n"
          + "          \"ackDeadlineSeconds\": 30\n"
          + "        }]\n"
          + "      }]\n"
          + "    }]\n"
          + "  }\n"
          + "}";
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void load() throws IOException {
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), CONFIG.getBytes(UTF_8));

    ConfigurationRepository configurationRepository = FileConfigurationRepository.create(file);
    assertThat(configurationRepository.getTopics("projects/project-1"), Matchers.hasSize(2));
    assertThat(configurationRepository.getTopics("projects/project-2"), Matchers.hasSize(2));
    assertThat(configurationRepository.getSubscriptions("projects/project-1"), Matchers.hasSize(4));
    assertThat(configurationRepository.getSubscriptions("projects/project-2"), Matchers.hasSize(3));
  }

  @Test
  public void load_missingFile() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unable to read Configuration from /tmp/file/doesnotexist.txt");

    FileConfigurationRepository.create(new File("/tmp/file/doesnotexist.txt"));
  }

  @Test
  public void load_invalidFormat() throws IOException {
    String partialContent =
        "{\n"
            + "  \"server\": {\n"
            + "    \"port\": 8080,\n"
            + "    \"security\": {\n"
            + "      \"certificateChainFile\": \"/path/to/server.crt\",\n"
            + "      \"privateKeyFile\": \"/path/to/server.key\"\n"
            + "    }\n"
            + "  }";
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), partialContent.getBytes(UTF_8));
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid Configuration read from " + file.getAbsolutePath());

    FileConfigurationRepository.create(file);
  }

  @Test
  public void save() throws IOException {
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), CONFIG.getBytes(UTF_8));

    ConfigurationRepository configurationRepository = FileConfigurationRepository.create(file);

    Files.write(file.toPath(), "".getBytes(UTF_8));
    assertThat(Files.readAllLines(file.toPath(), UTF_8), Matchers.empty());

    configurationRepository.save();
    String content = String.join("\n", Files.readAllLines(file.toPath(), UTF_8));
    assertThat(content, Matchers.equalTo(CONFIG));
  }

  @Test
  public void save_afterChanges()
      throws IOException, ConfigurationAlreadyExistsException, ConfigurationNotFoundException {
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), CONFIG.getBytes(UTF_8));

    ConfigurationRepository configurationRepository = FileConfigurationRepository.create(file);

    com.google.pubsub.v1.Topic newTopic =
        com.google.pubsub.v1.Topic.newBuilder()
            .setName("projects/new-project/topics/new-topic")
            .putLabels(KAFKA_TOPIC, "kafka-new-topic")
            .build();
    configurationRepository.createTopic(newTopic);
    Subscription newSubscription =
        Subscription.newBuilder()
            .setName("projects/new-project/subscriptions/new-subscription")
            .setTopic("projects/new-project/topics/new-topic")
            .build();
    configurationRepository.createSubscription(newSubscription);

    configurationRepository.save();
    String content = String.join("\n", Files.readAllLines(file.toPath(), UTF_8));
    assertThat(
        content,
        Matchers.equalTo(
            "{\n"
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
                + "      \"name\": \"new-project\",\n"
                + "      \"topics\": [{\n"
                + "        \"name\": \"new-topic\",\n"
                + "        \"kafkaTopic\": \"kafka-new-topic\",\n"
                + "        \"subscriptions\": [{\n"
                + "          \"name\": \"new-subscription\",\n"
                + "          \"ackDeadlineSeconds\": 10\n"
                + "        }]\n"
                + "      }]\n"
                + "    }, {\n"
                + "      \"name\": \"project-1\",\n"
                + "      \"topics\": [{\n"
                + "        \"name\": \"topic-1\",\n"
                + "        \"kafkaTopic\": \"kafka-topic-1\",\n"
                + "        \"subscriptions\": [{\n"
                + "          \"name\": \"subscription-1\",\n"
                + "          \"ackDeadlineSeconds\": 10\n"
                + "        }, {\n"
                + "          \"name\": \"subscription-2\",\n"
                + "          \"ackDeadlineSeconds\": 10\n"
                + "        }]\n"
                + "      }, {\n"
                + "        \"name\": \"topic-2\",\n"
                + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
                + "        \"subscriptions\": [{\n"
                + "          \"name\": \"subscription-3\",\n"
                + "          \"ackDeadlineSeconds\": 30\n"
                + "        }, {\n"
                + "          \"name\": \"subscription-4\",\n"
                + "          \"ackDeadlineSeconds\": 45\n"
                + "        }]\n"
                + "      }]\n"
                + "    }, {\n"
                + "      \"name\": \"project-2\",\n"
                + "      \"topics\": [{\n"
                + "        \"name\": \"topic-1\",\n"
                + "        \"kafkaTopic\": \"kafka-topic-1\",\n"
                + "        \"subscriptions\": [{\n"
                + "          \"name\": \"subscription-1\",\n"
                + "          \"ackDeadlineSeconds\": 10\n"
                + "        }, {\n"
                + "          \"name\": \"subscription-2\",\n"
                + "          \"ackDeadlineSeconds\": 10\n"
                + "        }]\n"
                + "      }, {\n"
                + "        \"name\": \"topic-2\",\n"
                + "        \"kafkaTopic\": \"kafka-topic-2\",\n"
                + "        \"subscriptions\": [{\n"
                + "          \"name\": \"subscription-3\",\n"
                + "          \"ackDeadlineSeconds\": 30\n"
                + "        }]\n"
                + "      }]\n"
                + "    }]\n"
                + "  }\n"
                + "}"));
  }

  @Test
  public void save_fileIsReadOnly() throws IOException {
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), CONFIG.getBytes(UTF_8));
    file.setWritable(false);

    ConfigurationRepository configurationRepository = FileConfigurationRepository.create(file);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage(file.getAbsolutePath() + " is not writeable.");
    configurationRepository.save();
  }
}
