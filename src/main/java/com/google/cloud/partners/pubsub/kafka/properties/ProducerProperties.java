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

import java.util.List;
import java.util.Properties;

public class ProducerProperties {

  private int executors = Runtime.getRuntime().availableProcessors() * 2;

  private List<String> topics;

  private Properties properties = new Properties();

  public int getExecutors() {
    return executors;
  }

  public void setExecutors(int executors) {
    this.executors = executors;
  }

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics(List<String> topics) {
    this.topics = topics;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "ProducerProperties{"
        + "executors='"
        + executors
        + '\''
        + ", topics="
        + topics
        + ", properties="
        + properties
        + '}';
  }
}
