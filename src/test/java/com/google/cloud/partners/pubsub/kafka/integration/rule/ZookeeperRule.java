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

package com.google.cloud.partners.pubsub.kafka.integration.rule;

import static org.junit.Assert.assertTrue;

import com.google.cloud.partners.pubsub.kafka.integration.util.EmbeddedZookeeper;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class ZookeeperRule extends ExternalResource {

  private final TemporaryFolder temporaryFolder;
  private final int replicationFactor;

  private final ExecutorService executorService;

  private final Map<Integer, EmbeddedZookeeper> zkServers = new ConcurrentHashMap<>();

  public ZookeeperRule(TemporaryFolder temporaryFolder, int replicationFactor) {
    this.temporaryFolder = temporaryFolder;
    this.replicationFactor = replicationFactor;
    this.executorService = Executors.newFixedThreadPool(replicationFactor);
  }

  @Override
  protected void before() throws Exception {
    for (int i = 0; i < replicationFactor; i++) {
      EmbeddedZookeeper start =
          EmbeddedZookeeper.builder(executorService, temporaryFolder, replicationFactor).start(i);
      zkServers.put(i, start);
    }

    zkServers.forEach(
        (key, embeddedZookeeper) ->
            assertTrue("Waiting for server being up", embeddedZookeeper.waitForServerUp()));
  }

  @Override
  protected void after() {
    zkServers.values().forEach(EmbeddedZookeeper::shutdown);
  }

  public Map<Integer, EmbeddedZookeeper> getZkServers() {
    return zkServers;
  }
}
