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

import static com.google.common.collect.Maps.newHashMap;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ApplicationProperties {

  @JsonProperty("server")
  private ServerProperties serverProperties = new ServerProperties();

  @JsonProperty("kafka")
  private KafkaProperties kafkaProperties;

  @JsonProperty("pubsub")
  private Map<String, List<PubSubBindProperties>> pubSubProperties = newHashMap();

  public ServerProperties getServerProperties() {
    return serverProperties;
  }

  public void setServerProperties(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  public KafkaProperties getKafkaProperties() {
    return kafkaProperties;
  }

  public void setKafkaProperties(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  public Map<String, List<PubSubBindProperties>> getPubSubProperties() {
    return pubSubProperties;
  }

  public void setPubSubProperties(Map<String, List<PubSubBindProperties>> pubSubProperties) {
    this.pubSubProperties = pubSubProperties;
  }

  @Override
  public String toString() {
    return "ApplicationProperties{"
        + "serverProperties='"
        + serverProperties
        + '\''
        + ", kafkaProperties="
        + kafkaProperties
        + ", pubSubProperties="
        + pubSubProperties
        + '}';
  }
}
