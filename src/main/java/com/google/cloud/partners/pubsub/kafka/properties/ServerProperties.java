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

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerProperties {

  private int port = 8080;

  private int parallelism = Runtime.getRuntime().availableProcessors();

  private String threadGroup = "pubsubemulator-grpc-threads";

  private String threadPrefix = "pubsubemulator-executor";

  @JsonProperty("security")
  private SecurityProperties securityProperties;

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public String getThreadGroup() {
    return threadGroup;
  }

  public void setThreadGroup(String threadGroup) {
    this.threadGroup = threadGroup;
  }

  public String getThreadPrefix() {
    return threadPrefix;
  }

  public void setThreadPrefix(String threadPrefix) {
    this.threadPrefix = threadPrefix;
  }

  public SecurityProperties getSecurityProperties() {
    return securityProperties;
  }

  public void setSecurityProperties(SecurityProperties securityProperties) {
    this.securityProperties = securityProperties;
  }

  @Override
  public String toString() {
    return "ServerProperties{"
        + "port='"
        + port
        + '\''
        + ", parallelism="
        + parallelism
        + ", threadGroup="
        + threadGroup
        + ", threadPrefix="
        + threadPrefix
        + ", securityProperties="
        + securityProperties
        + '}';
  }
}
