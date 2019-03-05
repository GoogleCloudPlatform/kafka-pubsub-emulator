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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.partners.pubsub.kafka.config.ConfigurationManager;
import com.google.cloud.partners.pubsub.kafka.config.PubSubFileRepository;
import com.google.cloud.partners.pubsub.kafka.config.PubSubRepository;
import com.google.cloud.partners.pubsub.kafka.config.Server;
import com.google.cloud.partners.pubsub.kafka.config.Server.Builder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.services.HealthStatusManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;

/** Default Guice dependency-injection module */
public class DefaultModule extends AbstractModule {

  private final File configurationFile;
  private final File pubSubFile;

  public DefaultModule(String configFilePath, String pubSubFilePath) {
    this.configurationFile = new File(configFilePath);
    this.pubSubFile = new File(pubSubFilePath);
  }

  @Override
  protected void configure() {
    bind(KafkaClientFactory.class).to(DefaultKafkaClientFactory.class);
    bind(Clock.class).toInstance(Clock.systemUTC());

    bind(ConfigurationManager.class);
    bind(SubscriptionManagerFactory.class);
    bind(PublisherService.class);
    bind(SubscriberService.class);
    bind(AdminService.class);
    bind(StatisticsManager.class);
    bind(HealthStatusManager.class);
    bind(PubsubEmulatorServer.class);
  }

  @Provides
  Server provideServer() {
    String json;
    try {
      json = String.join("\n", Files.readAllLines(configurationFile.toPath(), UTF_8));
      Builder builder = Server.newBuilder();
      return ConfigurationManager.parseFromJson(builder, json).build();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to read Server configuration from " + configurationFile.getAbsolutePath(), e);
    }
  }

  @Provides
  PubSubRepository providePubSubRepository() {
    return new PubSubFileRepository(pubSubFile);
  }
}
