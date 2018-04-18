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

package com.google.cloud.partners.pubsub.kafka.integration.rule;

import static java.util.Collections.singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.google.cloud.partners.pubsub.kafka.integration.util.EmbeddedKafka;
import com.google.cloud.partners.pubsub.kafka.integration.util.EmbeddedZookeeper;
import com.google.common.collect.ImmutableMap;

public class KafkaRule extends ExternalResource {

  private static final Logger LOGGER = Logger.getLogger(KafkaRule.class.getName());

  private static final int INIT_NODE_ID = 0;

  private final TemporaryFolder temporaryFolder;

  private final String zkConnect;

  private final int replicationFactor;
  private final Map<Integer, EmbeddedKafka> kafkaServers = new ConcurrentHashMap<>();

  public KafkaRule(
      TemporaryFolder temporaryFolder, ZookeeperRule zookeeperRule, int replicationFactor) {
    this.temporaryFolder = temporaryFolder;
    this.replicationFactor = replicationFactor;

    zkConnect =
        zookeeperRule
            .getZkServers()
            .values()
            .stream()
            .map(EmbeddedZookeeper::getZkConnect)
            .collect(Collectors.joining(","));
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  @Override
  protected void before() throws Throwable {
    for (int nodeId = INIT_NODE_ID; nodeId < replicationFactor; nodeId++) {
      start(nodeId);
    }
  }

  @Override
  protected void after() {
    kafkaServers.keySet().forEach(this::shutdown);
  }

  public void createTopic(String name) throws Exception {
    try (AdminClient admin = AdminClient.create(getAdminConfig())) {
      int partitions = 3;
      int replication = this.replicationFactor;
      Matcher matcher = Pattern.compile("(-\\d+[p|r])").matcher(name);
      while (matcher.find()) {
        String group = matcher.group();
        if (group.endsWith("p")) {
          partitions = getNumber(group);
        } else {
          replication = getNumber(group);
        }
      }

      NewTopic topic = new NewTopic(name, partitions, (short) replication);
      CreateTopicsResult topicsResult = admin.createTopics(singleton(topic));
      topicsResult.all().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error on create topics " + name, e);
      throw e;
    }
  }

  private Integer getNumber(String group) {
    return Integer.valueOf(group.replaceAll("[^\\d]", ""));
  }

  public void deleteTopics(String... topics) throws Exception {
    try (AdminClient admin = AdminClient.create(getAdminConfig())) {
      DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(Arrays.asList(topics));
      deleteTopicsResult.all().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error on delete topics " + Arrays.toString(topics), e);
      throw e;
    }
  }

  public Integer getNodeIdOfLeader(String topicName) throws Exception {
    try (AdminClient admin = AdminClient.create(getAdminConfig())) {
      return admin
          .describeTopics(Collections.singletonList(topicName))
          .values()
          .get(topicName)
          .get()
          .partitions()
          .stream()
          .collect(Collectors.groupingBy(x -> x.leader().id(), Collectors.counting()))
          .entrySet()
          .stream()
          .max(Entry.comparingByValue())
          .map(Entry::getKey)
          .orElse(INIT_NODE_ID);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error on get node id of leader by topic " + topicName, e);
      throw e;
    }
  }

  public void shutdown(Integer nodeId) {
    kafkaServers.get(nodeId).shutdown();
    kafkaServers.remove(nodeId);
  }

  public void start(Integer nodeId) throws Exception {
    if (kafkaServers.containsKey(nodeId)) {
      throw new IllegalArgumentException("NodeId already exists");
    }

    EmbeddedKafka embeddedKafka =
        EmbeddedKafka.builder(temporaryFolder, replicationFactor, zkConnect).start(nodeId);

    kafkaServers.put(nodeId, embeddedKafka);
  }

  private Map<String, Object> getAdminConfig() {
    String bootstrapServers =
        kafkaServers
            .values()
            .stream()
            .map(EmbeddedKafka::getKafkaConnect)
            .collect(Collectors.joining(","));

    return ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
  }
}
