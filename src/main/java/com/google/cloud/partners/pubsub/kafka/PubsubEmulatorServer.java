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

import static io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.cloud.partners.pubsub.kafka.common.AdminGrpc;
import com.google.cloud.partners.pubsub.kafka.config.ConfigurationRepository;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.services.HealthStatusManager;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * An implementation of the Cloud Pub/Sub service built on top of Apache Kafka's messaging system.
 */
@Singleton
public class PubsubEmulatorServer {

  private static final Logger LOGGER = Logger.getLogger(PubsubEmulatorServer.class.getName());
  private static final int MAX_MESSAGE_SIZE = 1000 * 1000 * 10; // 10MB

  private final PublisherService publisher;
  private final SubscriberService subscriber;
  private final HealthStatusManager healthStatusManager;
  private final Server server;

  @Inject
  public PubsubEmulatorServer(
      ConfigurationRepository configurationRepository,
      PublisherService publisher,
      SubscriberService subscriber,
      AdminService admin,
      HealthStatusManager healthStatusManager) {
    this.publisher = publisher;
    this.subscriber = subscriber;
    this.healthStatusManager = healthStatusManager;

    ServerBuilder builder =
        ServerBuilder.forPort(configurationRepository.getServer().getPort())
            .addService(publisher)
            .addService(subscriber)
            .addService(admin)
            .addService(healthStatusManager.getHealthService())
            .maxInboundMessageSize(MAX_MESSAGE_SIZE);
    if (configurationRepository.getServer().hasSecurity()) {
      builder.useTransportSecurity(
          new File(configurationRepository.getServer().getSecurity().getCertificateChainFile()),
          new File(configurationRepository.getServer().getSecurity().getPrivateKeyFile()));
    }
    server = builder.build();
  }

  /**
   * Initialize and start the PubsubEmulatorServer.
   *
   * <p>To set an external configuration file must be considered argument
   * `configuration.location=/to/path/application.yaml` the properties will be merged.
   */
  public static void main(String[] args) {
    Args argObject = new Args();
    JCommander jCommander = JCommander.newBuilder().addObject(argObject).build();
    jCommander.parse(args);
    if (argObject.help) {
      jCommander.usage();
      return;
    }
    Injector injector = Guice.createInjector(new DefaultModule(argObject.configurationFile));
    PubsubEmulatorServer pubsubEmulatorServer = injector.getInstance(PubsubEmulatorServer.class);
    try {
      pubsubEmulatorServer.start();
      pubsubEmulatorServer.blockUntilShutdown();
    } catch (IOException | InterruptedException e) {
      LOGGER.log(Level.SEVERE, "Unexpected server failure", e);
    }
  }

  /** Start the server and add a hook that calls {@link #stop()} when the JVM is shutting down. */
  public void start() throws IOException {
    server.start();
    startHealthcheckServices();
    LOGGER.info("PubsubEmulatorServer started on port " + server.getPort());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println(
                      "*** shutting down gRPC PubsubEmulatorServer since JVM is shutting down");
                  PubsubEmulatorServer.this.stop();
                  System.err.println("*** server shut down");
                }));
  }

  /** Stop serving requests, then shutdown Publisher and Subscriber services. */
  public void stop() {
    if (server != null) {
      server.shutdownNow();
    }
    publisher.shutdown();
    subscriber.shutdown();
  }

  /** Await termination on the main thread since the gRPC library uses daemon threads. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /** Add status information to healthcheck for each service. */
  private void startHealthcheckServices() {
    healthStatusManager.setStatus(PublisherGrpc.SERVICE_NAME, SERVING);
    healthStatusManager.setStatus(SubscriberGrpc.SERVICE_NAME, SERVING);
    healthStatusManager.setStatus(AdminGrpc.SERVICE_NAME, SERVING);
  }

  /** Command-line arguments. */
  @Parameters(separators = "=")
  private static final class Args {
    @Parameter(
        names = {"--help"},
        help = true)
    private boolean help = false;

    @Parameter(
        names = {"-c", "--configuration-file"},
        required = true,
        description = "Path to a JSON-formatted configuration file.")
    private String configurationFile;
  }
}
