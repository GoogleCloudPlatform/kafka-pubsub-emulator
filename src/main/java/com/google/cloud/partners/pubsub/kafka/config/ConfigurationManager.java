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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
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
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Manages the emulator's server configuration, and communicates with the Pub/Sub repository used
 * for retrieving and persisting Projects, Topics, and Subscriptions.
 */
@Singleton
public final class ConfigurationManager {

  public static final String KAFKA_TOPIC = "kafka-topic";
  public static final String KAFKA_TOPIC_SEPARATOR = ".";

  private final SetMultimap<String, com.google.pubsub.v1.Topic> topicsByProject =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private final SetMultimap<String, com.google.pubsub.v1.Subscription> subscriptionsByProject =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private final Server serverConfiguration;
  private final PubSubRepository pubSubRepository;

  @Inject
  public ConfigurationManager(Server serverConfiguration, PubSubRepository pubSubRepository) {
    Preconditions.checkNotNull(serverConfiguration);
    Preconditions.checkNotNull(pubSubRepository);

    this.serverConfiguration = serverConfiguration;
    this.pubSubRepository = pubSubRepository;
    topicsByProject.clear();
    subscriptionsByProject.clear();
    for (Project p : pubSubRepository.load().getProjectsList()) {
      for (Topic t : p.getTopicsList()) {
        String projectName = ProjectName.format(p.getName());
        String topicName = ProjectTopicName.format(p.getName(), t.getName());
        String kafkaTopic = Optional.ofNullable(t.getKafkaTopic()).orElse(t.getName());
        topicsByProject.put(
            projectName,
            com.google.pubsub.v1.Topic.newBuilder()
                .setName(topicName)
                .putLabels(KAFKA_TOPIC, kafkaTopic)
                .build());

        for (Subscription s : t.getSubscriptionsList()) {
          subscriptionsByProject.put(
              projectName,
              com.google.pubsub.v1.Subscription.newBuilder()
                  .setTopic(topicName)
                  .setName(ProjectSubscriptionName.format(p.getName(), s.getName()))
                  .setAckDeadlineSeconds(s.getAckDeadlineSeconds())
                  .putLabels(KAFKA_TOPIC, kafkaTopic)
                  .build());
        }
      }
    }
  }

  /** Populates the provided protobuf builder object from a JSON string. */
  public static <T extends Message.Builder> T parseFromJson(T builder, String json) {
    try {
      JsonFormat.parser().merge(json, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Unable to parse protobuf message from JSON", e);
    }
  }

  /** Returns the List of Projects */
  public final List<String> getProjects() {
    return Stream.concat(
            topicsByProject.keySet().stream(), subscriptionsByProject.keySet().stream())
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  /** Returns the Topic object with the given name if it exists */
  public final Optional<com.google.pubsub.v1.Topic> getTopicByName(String topicName) {
    return topicsByProject
        .values()
        .stream()
        .filter(topic -> topic.getName().equals(topicName))
        .findFirst();
  }

  /** Returns Subscription object with the given name if it exists */
  public final Optional<com.google.pubsub.v1.Subscription> getSubscriptionByName(
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

  /** Returns the Server configuration. */
  public final Server getServer() {
    return serverConfiguration;
  }

  /** Updates the managed Server when a new Topic is created. */
  public final com.google.pubsub.v1.Topic createTopic(com.google.pubsub.v1.Topic topic)
      throws ConfigurationAlreadyExistsException {
    ProjectTopicName projectTopicName = ProjectTopicName.parse(topic.getName());
    if (getTopicByName(topic.getName()).isPresent()) {
      throw new ConfigurationAlreadyExistsException(
          "Topic " + projectTopicName.toString() + " already exists");
    }
    com.google.pubsub.v1.Topic.Builder builder = topic.toBuilder();
    if (topic.getLabelsOrDefault(KAFKA_TOPIC, null) == null) {
      builder.putLabels(
          KAFKA_TOPIC,
          String.join(
              KAFKA_TOPIC_SEPARATOR, projectTopicName.getProject(), projectTopicName.getTopic()));
    }

    com.google.pubsub.v1.Topic built = builder.build();
    topicsByProject.put(ProjectName.of(projectTopicName.getProject()).toString(), built);
    pubSubRepository.save(getPubSub());
    return built;
  }

  /** Updates the managed Server when a Topic is deleted. */
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
    pubSubRepository.save(getPubSub());
  }

  /** Updates the managed Server when a new Subscription is created. */
  public final com.google.pubsub.v1.Subscription createSubscription(
      com.google.pubsub.v1.Subscription subscription)
      throws ConfigurationAlreadyExistsException, ConfigurationNotFoundException {
    ProjectTopicName projectTopicName = ProjectTopicName.parse(subscription.getTopic());
    ProjectSubscriptionName projectSubscriptionName =
        ProjectSubscriptionName.parse(subscription.getName());

    if (getSubscriptionByName(subscription.getName()).isPresent()) {
      throw new ConfigurationAlreadyExistsException(
          "Subscription " + projectSubscriptionName.toString() + " already exists");
    }
    com.google.pubsub.v1.Topic topic =
        getTopicByName(projectTopicName.toString())
            .orElseThrow(
                () ->
                    new ConfigurationNotFoundException(
                        "Topic " + projectTopicName.toString() + " does not exist"));
    com.google.pubsub.v1.Subscription.Builder builder =
        subscription.toBuilder().putLabels(KAFKA_TOPIC, topic.getLabelsOrThrow(KAFKA_TOPIC));
    if (subscription.getAckDeadlineSeconds() == 0) {
      builder.setAckDeadlineSeconds(10).build();
    }
    com.google.pubsub.v1.Subscription built = builder.build();
    subscriptionsByProject.put(
        ProjectName.of(projectSubscriptionName.getProject()).toString(), built);
    pubSubRepository.save(getPubSub());
    return built;
  }

  /** Updates the managed Server when a Subscription is deleted. */
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
    pubSubRepository.save(getPubSub());
  }

  /**
   * Generates and returns a new PubSub object based on the managed state in the repository.
   * Projects, Topics, and Subscriptions will all be added in sorted order by name
   */
  public final PubSub getPubSub() {
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

    return PubSub.newBuilder()
        .addAllProjects(
            projectMap
                .values()
                .stream()
                .sorted(Comparator.comparing(Project.Builder::getName))
                .map(Builder::build)
                .collect(Collectors.toList()))
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
