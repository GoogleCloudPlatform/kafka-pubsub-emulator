package com.google.cloud.partners.pubsub.kafka.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.partners.pubsub.kafka.config.PubSub.Builder;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javax.inject.Singleton;

/**
 * Implementation of {@link PubSubRepository} that expects to be provided with a File object
 * pointing to a JSON-formatted {@link PubSub} protobuf. The File must be writeable to allow for
 * state mutating operations (ie. New Topic) to succeed.
 *
 * <p>TODO: Support proto text format as well
 */
@Singleton
public final class PubSubFileRepository implements PubSubRepository {

  private final File file;

  public PubSubFileRepository(File file) {
    this.file = file;
  }

  @Override
  public PubSub load() {
    String json;
    try {
      json = String.join("\n", Files.readAllLines(file.toPath(), UTF_8));
      Builder builder = PubSub.newBuilder();
      return ConfigurationManager.parseFromJson(builder, json).build();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to read PubSub configuration from " + file.getAbsolutePath(), e);
    }
  }

  @Override
  public void save(PubSub pubSub) {
    try {
      Files.write(file.toPath(), JsonFormat.printer().print(pubSub).getBytes(UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected error when saving PubSub configuration to " + file.getAbsolutePath() + ".",
          e);
    }
  }
}
