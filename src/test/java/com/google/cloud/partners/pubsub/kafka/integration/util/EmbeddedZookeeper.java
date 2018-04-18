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

package com.google.cloud.partners.pubsub.kafka.integration.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.sasl.SaslException;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.TemporaryFolder;

public class EmbeddedZookeeper {

  private static final Logger LOGGER = Logger.getLogger(EmbeddedZookeeper.class.getName());

  private static final int CONNECTION_TIMEOUT = 30000;

  private static final int DEFAULT_SOCKET_TIMEOUT = 5000;

  private static final int INIT_PORT = 2181;

  private static final int INIT_SERVER = 2888;

  private static final int INIT_LEADER_ELECTION = 3888;

  private final TemporaryFolder temporaryFolder;

  private final Integer replicationFactor;

  private final ExecutorService executorService;

  private final QuorumPeerConfig configuration = new QuorumPeerConfig();

  private final QuorumPeer quorumPeer;
  private final CountDownLatch finished = new CountDownLatch(1);

  private EmbeddedZookeeper(
      ExecutorService executorService, TemporaryFolder temporaryFolder, Integer replicationFactor)
      throws SaslException {
    this.temporaryFolder = temporaryFolder;
    this.replicationFactor = replicationFactor;
    this.executorService = executorService;
    quorumPeer = QuorumPeer.testingQuorumPeer();
  }

  public static EmbeddedZookeeper builder(
      ExecutorService executorService, TemporaryFolder temporaryFolder, Integer replicationFactor)
      throws SaslException {
    return new EmbeddedZookeeper(executorService, temporaryFolder, replicationFactor);
  }

  public EmbeddedZookeeper start(Integer position) {
    // Sporadic failures creating temporary space, try up to 3 times
    for (int attempts = 3; attempts > 0; attempts--) {
      try {
        File dataDir = temporaryFolder.newFolder();
        File logDir = Files.createDirectory(dataDir.toPath().resolve("log")).toFile();
        Path myid = Files.createFile(dataDir.toPath().resolve("myid"));
        Files.write(myid, position.toString().getBytes());
        FileTxnSnapLog fileTxnSnapLog = new FileTxnSnapLog(dataDir, logDir);

        Integer port = INIT_PORT + position;

        Properties zkProperties = new Properties();
        zkProperties.setProperty("tickTime", "2000");
        zkProperties.setProperty("dataDir", dataDir.getCanonicalPath());
        zkProperties.setProperty("dataLogDir", logDir.getCanonicalPath());
        zkProperties.setProperty("clientPort", port.toString());
        zkProperties.setProperty("initLimit", "5");
        zkProperties.setProperty("syncLimit", "2");
        zkProperties.setProperty("zookeeper.jmx.log4j.disable", "true");
        for (int i = 0; i < replicationFactor; i++) {
          zkProperties.setProperty(
              "server." + i,
              String.format("localhost:%s:%s", INIT_SERVER + i, INIT_LEADER_ELECTION + i));
        }

        // Start and schedule the the purge task
        DatadirCleanupManager purgeMgr =
            new DatadirCleanupManager(dataDir.toString(), logDir.toString(), 24, 5);
        purgeMgr.start();

        configuration.parseProperties(zkProperties);

        LOGGER.info("Starting quorum peer");
        ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
        cnxnFactory.configure(
            configuration.getClientPortAddress(), configuration.getMaxClientCnxns());

        quorumPeer.setQuorumPeers(configuration.getServers());
        quorumPeer.setTxnFactory(fileTxnSnapLog);
        quorumPeer.setElectionType(configuration.getElectionAlg());
        quorumPeer.setMyid(position);
        quorumPeer.setTickTime(2000);
        quorumPeer.setInitLimit(5);
        quorumPeer.setSyncLimit(2);
        quorumPeer.setQuorumListenOnAllIPs(configuration.getQuorumListenOnAllIPs());
        quorumPeer.setCnxnFactory(cnxnFactory);
        quorumPeer.setQuorumVerifier(configuration.getQuorumVerifier());
        quorumPeer.setClientPortAddress(configuration.getClientPortAddress());
        quorumPeer.setMinSessionTimeout(configuration.getMinSessionTimeout());
        quorumPeer.setMaxSessionTimeout(configuration.getMaxSessionTimeout());
        quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
        quorumPeer.setLearnerType(configuration.getPeerType());
        quorumPeer.setSyncEnabled(configuration.getSyncEnabled());

        executorService.submit(
            () -> {
              try {
                quorumPeer.initialize();
                quorumPeer.start();
                quorumPeer.join();
              } catch (InterruptedException | SaslException e) {
                // warn, but generally this is ok
                LOGGER.log(Level.WARNING, "Quorum Peer interrupted", e);
              } finally {
                finished.countDown();
              }
            });

        return this;
      } catch (IOException | QuorumPeerConfig.ConfigException e) {
        LOGGER.warning("Unable to start Zookeeper, " + (attempts - 1) + " attempts remaining");
      }
    }
    throw new RuntimeException("Unable to start Zookeeper after exhausting all retries");
  }

  public void shutdown() {
    quorumPeer.shutdown();
    try {
      finished.await();
    } catch (InterruptedException ignored) {
    }
  }

  public String getZkConnect() {
    InetSocketAddress clientPortAddress = configuration.getClientPortAddress();
    return String.format("%s:%s", clientPortAddress.getHostName(), clientPortAddress.getPort());
  }

  public boolean waitForServerUp() {
    long start = currentElapsedTime();
    while (true) {
      try {
        String result = send4LetterWord();
        if (result.startsWith("Zookeeper version:") && !result.contains("READ-ONLY")) {
          return true;
        }
      } catch (IOException e) {
        InetSocketAddress clientPortAddress = configuration.getClientPortAddress();
        LOGGER.log(
            Level.SEVERE,
            String.format(
                "Zookeeper is not running in %s:%s",
                clientPortAddress.getHostName(), clientPortAddress.getPort()));
      }

      if (currentElapsedTime() > start + CONNECTION_TIMEOUT) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  private long currentElapsedTime() {
    return System.nanoTime() / 1000000;
  }

  private String send4LetterWord() throws IOException {
    LOGGER.log(
        Level.INFO,
        "Connecting {0}:{1} to check if the Zookeeper is running",
        new String[] {
          configuration.getClientPortAddress().getHostName(),
          String.valueOf(configuration.getClientPortAddress().getPort())
        });
    BufferedReader reader = null;
    try (Socket sock = new Socket()) {
      sock.setSoTimeout(DEFAULT_SOCKET_TIMEOUT);
      sock.connect(configuration.getClientPortAddress(), DEFAULT_SOCKET_TIMEOUT);

      OutputStream outputStream = sock.getOutputStream();
      outputStream.write("stat".getBytes());
      outputStream.flush();

      sock.shutdownOutput();

      reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }
      return sb.toString();
    } catch (SocketTimeoutException e) {
      throw new IOException("Exception while executing four letter word: stat", e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
