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

import java.util.concurrent.atomic.LongAdder;

/**
 * Class to consolidate information of emulator, contain some calculation rules.
 */
public class StatisticsInformation {

  private final LongAdder count = new LongAdder();

  private final LongAdder bytes = new LongAdder();

  private final LongAdder latency = new LongAdder();

  private final LongAdder error = new LongAdder();

  public void compute(long latency, int bytes) {
    this.latency.add(latency);
    this.bytes.add(bytes);
    this.count.add(1);
  }

  public void computeError() {
    this.error.add(1);
  }

  public LongAdder getCount() {
    return count;
  }

  public Float getThroughput(long durationSeconds) {
    return this.bytes.floatValue() / durationSeconds;
  }

  public Float getAverageLatency() {
    return this.count.intValue() == 0 ? 0 : this.latency.floatValue() / this.count.floatValue();
  }

  public Float getQPS(long durationSeconds) {
    return this.count.floatValue() / (float) durationSeconds;
  }

  public Float getErrorRating() {
    LongAdder totalMessages = new LongAdder();
    totalMessages.add(this.error.intValue());
    totalMessages.add(this.count.intValue());
    if (totalMessages.intValue() == 0) {
      return 0F;
    }
    return (this.error.floatValue() / totalMessages.floatValue()) * 100;
  }
}
