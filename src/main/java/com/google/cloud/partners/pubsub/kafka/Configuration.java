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

import static com.google.common.collect.Lists.newArrayList;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.partners.pubsub.kafka.properties.ApplicationProperties;
import com.google.cloud.partners.pubsub.kafka.properties.ConsumerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.ProducerProperties;
import com.google.cloud.partners.pubsub.kafka.properties.PubSubBindProperties;

public class Configuration {

  private static final ObjectMapper MAPPER = getMapper();
  public static final String RESOURCE_CHAR_SEPARATOR = "/";

  private static ApplicationProperties properties;

  private Configuration() {
  }

  public static void loadApplicationProperties(String location) {
    try {
      ApplicationProperties loadProperties =
          MAPPER.readValue(new File(location), ApplicationProperties.class);
      validateMandatoryProperties(loadProperties);
      properties = loadProperties;
    } catch (IOException e) {
      throw new IllegalArgumentException("Error loading application configuration.", e);
    }
  }

  public static ApplicationProperties getApplicationProperties() {
    return properties;
  }

  private static void validateMandatoryProperties(ApplicationProperties properties) {
    ConsumerProperties consumerProperties = properties.getKafkaProperties().getConsumerProperties();
    ProducerProperties producerProperties = properties.getKafkaProperties().getProducerProperties();

    if (isEmpty(consumerProperties.getSubscriptions()) && isEmpty(producerProperties.getTopics())) {
      throw new IllegalArgumentException("Must provide at least one topic or one subscription.");
    }
  }

  private static ObjectMapper getMapper() {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper;
  }

  private static boolean isEmpty(Collection collection) {
    return collection == null || collection.isEmpty();
  }

  public static String getCurrentConfiguration() throws JsonProcessingException {
    return MAPPER.writeValueAsString(properties);
  }

  /**
   * If the topic contains the projects/{project}/topics/{topic} format, strip out the prefix and
   * return only the final portion which is the topic's name.
   *
   * @param topicPath topic information
   * @return last information based on split logic
   */
  public static String getLastNodeInTopic(String topicPath) {
    return getLastNode(topicPath, PubSubBindProperties::getTopic);
  }

  /**
   * If the topic contains the projects/{project}/subscriptions/{subscription} format, strip out the
   * prefix and return only the final portion which is the subscriptions's name.
   *
   * @param subscriptionPath name of topic on subscriptions
   */
  public static String getLastNodeInSubscription(String subscriptionPath) {
    return getLastNode(subscriptionPath, PubSubBindProperties::getSubscription);
  }

  private static String getLastNode(String path,
      Function<PubSubBindProperties, String> mapFunction) {
    if (path.contains(RESOURCE_CHAR_SEPARATOR)) {
      String[] pieces = path.split(RESOURCE_CHAR_SEPARATOR);
      Map<String, List<PubSubBindProperties>> pubSubProperties = properties.getPubSubProperties();
      String resource = pieces[pieces.length - 1];
      if (pubSubProperties.isEmpty()) {
        return resource;
      } else {
        String project = pieces[1];

        return pubSubProperties.getOrDefault(project, newArrayList())
            .stream()
            .map(mapFunction)
            .filter(Objects::nonNull)
            .filter(Predicate.isEqual(resource))
            .findFirst().orElse(path);
      }
    }
    return path;
  }


}
