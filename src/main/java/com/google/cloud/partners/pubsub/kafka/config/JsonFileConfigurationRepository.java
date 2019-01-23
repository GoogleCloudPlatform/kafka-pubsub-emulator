package com.google.cloud.partners.pubsub.kafka.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class JsonFileConfigurationRepository extends ConfigurationRepository {

  private final File file;

  public JsonFileConfigurationRepository(File file) {
    this.file = file;
    setConfiguration(load());
  }

  @Override
  public Configuration load() {
    String json;
    try {
      json = String.join("\n", Files.readAllLines(file.toPath(), UTF_8));
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to read Configuration from " + file.getAbsolutePath(), e);
    }

    try {
      Configuration.Builder builder = Configuration.newBuilder();
      JsonFormat.parser().merge(json, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Invalid Configuration read from " + file.getAbsolutePath(), e);
    }
  }

  @Override
  public void save() {
    if (!file.canWrite()) {
      throw new UnsupportedOperationException(
          "Configuration cannot be saved. " + file.getAbsolutePath() + " is not writeable.");
    }
    try {
      Files.write(file.toPath(), JsonFormat.printer().print(getConfiguration()).getBytes(UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected error when saving Configuration to " + file.getAbsolutePath() + ".", e);
    }
  }
}
