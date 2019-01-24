/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.partners.pubsub.kafka.config;

import com.google.cloud.partners.pubsub.kafka.config.Project.Builder;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Abstract class for managing emulator configuration properties. */
public abstract class ConfigurationRepository {

  static final String KAFKA_TOPIC = "kafka-topic";

  private final SetMultimap<String, com.google.pubsub.v1.Topic> topicsByProject =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private final SetMultimap<String, com.google.pubsub.v1.Subscription> subscriptionsByProject =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private final Configuration originalConfiguration;

  ConfigurationRepository(Configuration configuration) {
    Preconditions.checkNotNull(configuration);
    this.originalConfiguration = configuration;
    topicsByProject.clear();
    subscriptionsByProject.clear();
    for (Project p : configuration.getPubsub().getProjectsList()) {
      for (Topic t : p.getTopicsList()) {
        String projectName = ProjectName.format(p.getName());
        String topicName = ProjectTopicName.format(p.getName(), t.getName());
        topicsByProject.put(
            projectName,
            com.google.pubsub.v1.Topic.newBuilder()
                .setName(topicName)
                .putLabels(KAFKA_TOPIC, t.getKafkaTopic())
                .build());

        for (Subscription s : t.getSubscriptionsList()) {
          subscriptionsByProject.put(
              projectName,
              com.google.pubsub.v1.Subscription.newBuilder()
                  .setTopic(topicName)
                  .setName(ProjectSubscriptionName.format(p.getName(), s.getName()))
                  .setAckDeadlineSeconds(s.getAckDeadlineSeconds())
                  .build());
        }
      }
    }
  }

  /** Persists the managed Configuration to its store. */
  abstract void save();

  /** Returns the Kafka configuration section */
  public final Kafka getKafka() {
    return originalConfiguration.getKafka();
  }

  /** Returns the Kafka configuration section */
  public final Server getServer() {
    return originalConfiguration.getServer();
  }

  /** Returns the Topic object with the given name if it exists */
  public Optional<com.google.pubsub.v1.Topic> getTopicByName(String topicName) {
    return topicsByProject
        .values()
        .stream()
        .filter(topic -> topic.getName().equals(topicName))
        .findFirst();
  }

  /** Returns Subscription object with the given name if it exists */
  private Optional<com.google.pubsub.v1.Subscription> getSubscriptionByName(
      String subscriptionName) {
    return subscriptionsByProject
        .values()
        .stream()
        .filter(sub -> sub.getName().equals(subscriptionName))
        .findFirst();
  }

  /** Returns a list of Topic objects for the specified project name. */
  public final List<com.google.pubsub.v1.Topic> getTopics(String project) {
    return topicsByProject
        .get(project)
        .stream()
        .sorted(Comparator.comparing(com.google.pubsub.v1.Topic::getName))
        .collect(Collectors.toList());
  }

  /** Returns a list of Subscription objects for the specified project name. */
  public final List<com.google.pubsub.v1.Subscription> getSubscriptions(String project) {
    return subscriptionsByProject
        .get(project)
        .stream()
        .sorted(Comparator.comparing(com.google.pubsub.v1.Subscription::getName))
        .collect(Collectors.toList());
  }

  /** Returns a list of Subscription objects for the specified Topic name. */
  public final List<com.google.pubsub.v1.Subscription> getSubscriptionsForTopic(String topicName) {
    return subscriptionsByProject
        .values()
        .stream()
        .filter(subscription -> subscription.getTopic().equals(topicName))
        .sorted(Comparator.comparing(com.google.pubsub.v1.Subscription::getName))
        .collect(Collectors.toList());
  }

  /** Updates the managed Configuration when a new Topic is created. */
  public final void createTopic(com.google.pubsub.v1.Topic topic)
      throws ConfigurationAlreadyExistsException {
    ProjectTopicName projectTopicName = ProjectTopicName.parse(topic.getName());
    if (getTopicByName(topic.getName()).isPresent()) {
      throw new ConfigurationAlreadyExistsException(
          "Topic " + projectTopicName.toString() + " already exists");
    }
    if (topic.getLabelsOrDefault(KAFKA_TOPIC, null) == null) {
      topic = topic.toBuilder().putLabels(KAFKA_TOPIC, projectTopicName.getTopic()).build();
    }
    topicsByProject.put(ProjectName.of(projectTopicName.getProject()).toString(), topic);
  }

  /** Updates the managed Configuration when a Topic is deleted. */
  public final void deleteTopic(String topicName) throws ConfigurationNotFoundException {
    ProjectTopicName projectTopicName = ProjectTopicName.parse(topicName);
    ProjectName projectName = ProjectName.of(projectTopicName.getProject());
    Set<com.google.pubsub.v1.Topic> topics = topicsByProject.get(projectName.toString());
    getTopicByName(topicName)
        .map(topics::remove)
        .orElseThrow(
            () ->
                new ConfigurationNotFoundException(
                    "Topic " + projectTopicName.toString() + " does not exist"));
  }

  /** Updates the managed Configuration when a new Subscription is created. */
  public final void createSubscription(com.google.pubsub.v1.Subscription subscription)
      throws ConfigurationAlreadyExistsException, ConfigurationNotFoundException {
    ProjectTopicName projectTopicName = ProjectTopicName.parse(subscription.getTopic());
    ProjectSubscriptionName projectSubscriptionName =
        ProjectSubscriptionName.parse(subscription.getName());

    if (getSubscriptionByName(subscription.getName()).isPresent()) {
      throw new ConfigurationAlreadyExistsException(
          "Subscription " + projectSubscriptionName.toString() + " already exists");
    }
    getTopicByName(projectTopicName.toString())
        .orElseThrow(
            () ->
                new ConfigurationNotFoundException(
                    "Topic " + projectTopicName.toString() + " does not exist"));
    if (subscription.getAckDeadlineSeconds() == 0) {
      subscription = subscription.toBuilder().setAckDeadlineSeconds(10).build();
    }
    subscriptionsByProject.put(
        ProjectName.of(projectSubscriptionName.getProject()).toString(), subscription);
  }

  /** Updates the managed Configuration when a Subscription is deleted. */
  public final void deleteSubscription(String subscriptionName)
      throws ConfigurationNotFoundException {
    ProjectSubscriptionName projectSubscriptionName =
        ProjectSubscriptionName.parse(subscriptionName);
    ProjectName projectName = ProjectName.of(projectSubscriptionName.getProject());

    Set<com.google.pubsub.v1.Subscription> subscriptions =
        subscriptionsByProject.get(projectName.toString());
    getSubscriptionByName(subscriptionName)
        .map(subscriptions::remove)
        .orElseThrow(
            () ->
                new ConfigurationNotFoundException(
                    "Subscription " + projectSubscriptionName.toString() + " does not exist"));
  }

  /**
   * Generates and returns a new Configuration object based on the managed state in the repository.
   * Projects, Topics, and Subscriptions will all be added in sorted order by name
   */
  final Configuration getConfiguration() {
    Map<String, Project.Builder> projectMap = new HashMap<>();
    topicsByProject
        .values()
        .stream()
        .sorted(Comparator.comparing(com.google.pubsub.v1.Topic::getName))
        .forEach(
            t -> {
              ProjectTopicName projectTopicName = ProjectTopicName.parse(t.getName());
              if (!projectMap.containsKey(projectTopicName.getProject())) {
                projectMap.put(
                    projectTopicName.getProject(),
                    Project.newBuilder().setName(projectTopicName.getProject()));
              }
              Project.Builder projectBuilder = projectMap.get(projectTopicName.getProject());
              projectBuilder.addTopics(
                  Topic.newBuilder()
                      .setName(projectTopicName.getTopic())
                      .setKafkaTopic(t.getLabelsOrDefault(KAFKA_TOPIC, projectTopicName.getTopic()))
                      .addAllSubscriptions(
                          subscriptionsByProject
                              .values()
                              .stream()
                              .filter(s -> s.getTopic().equals(t.getName()))
                              .sorted(
                                  Comparator.comparing(com.google.pubsub.v1.Subscription::getName))
                              .map(
                                  s ->
                                      Subscription.newBuilder()
                                          .setName(
                                              ProjectSubscriptionName.parse(s.getName())
                                                  .getSubscription())
                                          .setAckDeadlineSeconds(s.getAckDeadlineSeconds())
                                          .build())
                              .collect(Collectors.toList()))
                      .build());
            });

    return originalConfiguration
        .toBuilder()
        .setPubsub(
            PubSub.newBuilder()
                .addAllProjects(
                    projectMap
                        .values()
                        .stream()
                        .sorted(Comparator.comparing(Project.Builder::getName))
                        .map(Builder::build)
                        .collect(Collectors.toList())))
        .build();
  }

  /** Thrown when the specified Project or Topic being requested is not found. */
  public static final class ConfigurationNotFoundException extends Exception {

    ConfigurationNotFoundException(String message) {
      super(message);
    }
  }

  /** Thrown when the specified Project or Topic being requested already exists. */
  public static final class ConfigurationAlreadyExistsException extends Exception {

    ConfigurationAlreadyExistsException(String message) {
      super(message);
    }
  }
}
