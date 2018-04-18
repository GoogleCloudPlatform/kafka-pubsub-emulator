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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ThreadFactory;

import org.junit.Test;

public class UtilsTest {

  @Test
  public void newThreadFactoryWithGroupAndPrefix() {
    ThreadFactory test = Utils.newThreadFactoryWithGroupAndPrefix("test-group", "test-thread");
    Thread t1 = test.newThread(() -> {});
    Thread t2 = test.newThread(() -> {});
    assertEquals("test-thread-0", t1.getName());
    assertEquals("test-group", t1.getThreadGroup().getName());
    assertEquals("test-thread-1", t2.getName());
    assertEquals("test-group", t2.getThreadGroup().getName());
  }
}
