package com.google.cloud.partners.pubsub.kafka.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.partners.pubsub.kafka.TestHelpers;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class PubSubFileRepositoryTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void load() throws IOException {
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), TestHelpers.PUBSUB_JSON.getBytes(UTF_8));

    PubSubFileRepository pubSubFileRepository = new PubSubFileRepository(file);
    assertThat(pubSubFileRepository.load(), Matchers.equalTo(TestHelpers.PUBSUB_CONFIG));
  }

  @Test
  public void load_missingFile() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Unable to read PubSub configuration from /tmp/file/doesnotexist.txt");

    new PubSubFileRepository(new File("/tmp/file/doesnotexist.txt")).load();
  }

  @Test
  public void load_invalidFormat() throws IOException {
    String partialContent =
        "{\n"
            + "  \"projects\":[\n"
            + "    {\n"
            + "      \"name\":\"project-1\",\n"
            + "      \"topics\":[\n"
            + "        {\n"
            + "          \"name\":\"topic-1\",\n"
            + "          \"kafkaTopic\":\"kafka-topic-1\",\n"
            + "          \"subscriptions\":[";
    File file = temporaryFolder.newFile();
    Files.write(file.toPath(), partialContent.getBytes(UTF_8));
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unable to parse protobuf message from JSON");

    new PubSubFileRepository(file).load();
  }

  @Test
  public void save() throws IOException {
    File file = temporaryFolder.newFile();

    PubSubFileRepository pubSubFileRepository = new PubSubFileRepository(file);
    pubSubFileRepository.save(TestHelpers.PUBSUB_CONFIG);

    String content = String.join("\n", Files.readAllLines(file.toPath(), UTF_8));
    assertThat(content, Matchers.equalTo(TestHelpers.PUBSUB_JSON));
  }

  @Test
  public void save_fileIsReadOnly() throws IOException {
    File file = temporaryFolder.newFile();
    file.setWritable(false);

    PubSubFileRepository pubSubFileRepository = new PubSubFileRepository(file);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Unexpected error when saving PubSub configuration to " + file.getAbsolutePath());

    pubSubFileRepository.save(TestHelpers.PUBSUB_CONFIG);
  }
}
