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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

final class Utils {

  /**
   * Return a new ThreadFactory instance that creates threads under the specified ThreadGroup name
   * and with the specified prefix.
   */
  public static ThreadFactory newThreadFactoryWithGroupAndPrefix(String groupName, String prefix) {
    return new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger(-1);
      private final ThreadGroup group = new ThreadGroup(groupName);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(group, r, prefix + "-" + counter.incrementAndGet());
      }
    };
  }
}
