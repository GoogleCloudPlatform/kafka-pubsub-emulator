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

package com.google.cloud.partners.pubsub.kafka.properties;

import static com.google.common.collect.Lists.newArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaProperties {

  private String bootstrapServers;

  @JsonProperty("producer")
  private ProducerProperties producerProperties = new ProducerProperties();

  @JsonProperty("consumer")
  private ConsumerProperties consumerProperties = new ConsumerProperties();

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public List<String> getListOfBootstrapServers() {
    return newArrayList(this.bootstrapServers.split(","));
  }

  public ProducerProperties getProducerProperties() {
    return producerProperties;
  }

  public void setProducerProperties(ProducerProperties producerProperties) {
    this.producerProperties = producerProperties;
  }

  public ConsumerProperties getConsumerProperties() {
    return consumerProperties;
  }

  public void setConsumerProperties(ConsumerProperties consumerProperties) {
    this.consumerProperties = consumerProperties;
  }

  @JsonIgnore
  public Set<String> getTopics() {
    Set<String> topics = Sets.newHashSet();
    topics.addAll(producerProperties.getTopics());
    topics.addAll(
        consumerProperties
            .getSubscriptions()
            .stream()
            .map(SubscriptionProperties::getTopic)
            .collect(Collectors.toSet()));
    return topics;
  }

  @Override
  public String toString() {
    return "KafkaProperties{"
        + "bootstrapServers='"
        + bootstrapServers
        + '\''
        + ", producerProperties="
        + producerProperties
        + ", consumerProperties="
        + consumerProperties
        + '}';
  }
}
