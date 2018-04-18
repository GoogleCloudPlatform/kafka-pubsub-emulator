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

package com.google.cloud.partners.pubsub.kafka.enums;

public enum MetricProperty {
  MESSAGE_COUNT("message_count", "Count of messages processed by emulator."),
  AVG_LATENCY("average_latency", "Average latency per request in milliseconds."),
  QPS("qps", "QPS."),
  THROUGHPUT("throughput", "Throughput in bytes per second"),
  ERROR_RATE("error_rate", "Percentage of requests resulting in errors.");

  private final String name;

  private final String description;

  MetricProperty(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }
}
