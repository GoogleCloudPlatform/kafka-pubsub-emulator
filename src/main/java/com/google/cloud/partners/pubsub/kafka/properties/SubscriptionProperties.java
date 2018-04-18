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

public class SubscriptionProperties {

  private String name;

  private String topic;

  private int ackDeadlineSeconds;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getAckDeadlineSeconds() {
    return ackDeadlineSeconds;
  }

  public void setAckDeadlineSeconds(int ackDeadlineSeconds) {
    this.ackDeadlineSeconds = ackDeadlineSeconds;
  }

  @Override
  public String toString() {
    return "SubscriptionProperties{"
        + "name='"
        + name
        + '\''
        + ", topic="
        + topic
        + ", ackDeadlineSeconds="
        + ackDeadlineSeconds
        + '}';
  }
}
