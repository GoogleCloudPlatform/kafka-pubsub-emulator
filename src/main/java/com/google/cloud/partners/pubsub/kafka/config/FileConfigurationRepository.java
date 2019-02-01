package com.google.cloud.partners.pubsub.kafka.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.flogger.FluentLogger;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javax.inject.Singleton;

/**
 * Implementation of {@link ConfigurationRepository} that expects to be provided with a File object
 * pointing to a JSON-formatted {@link Configuration} protobuf. If the file is writeable, the save
 * operation will serialize the in-memory state back to the same JSON file.
 *
 * <p>TODO: Support proto text format as well
 */
@Singleton
public class FileConfigurationRepository extends ConfigurationRepository {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final File file;
  private final boolean writeOnSave;

  private FileConfigurationRepository(Configuration configuration, File file, boolean writeOnSave) {
    super(configuration);
    this.file = file;
    this.writeOnSave = writeOnSave;
  }

  public static ConfigurationRepository create(File file, boolean writeOnSave) {
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
      return new FileConfigurationRepository(builder.build(), file, writeOnSave);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Invalid Configuration read from " + file.getAbsolutePath(), e);
    }
  }

  @Override
  void save() {
    if (writeOnSave) {
      try {
        Files.write(file.toPath(), JsonFormat.printer().print(getConfiguration()).getBytes(UTF_8));
      } catch (IOException e) {
        throw new IllegalStateException(
            "Unexpected error when saving Configuration to " + file.getAbsolutePath() + ".", e);
      }
    } else {
      logger.atInfo().log(
          "%s is not configured for writing on configuration changes", file.getAbsolutePath());
    }
  }
}
