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

import static kafka.server.KafkaConfig.AutoLeaderRebalanceEnableProp;
import static kafka.server.KafkaConfig.BrokerIdProp;
import static kafka.server.KafkaConfig.DeleteTopicEnableProp;
import static kafka.server.KafkaConfig.ListenersProp;
import static kafka.server.KafkaConfig.LogCleanupIntervalMsProp;
import static kafka.server.KafkaConfig.LogDirProp;
import static kafka.server.KafkaConfig.LogRetentionTimeHoursProp;
import static kafka.server.KafkaConfig.LogSegmentBytesProp;
import static kafka.server.KafkaConfig.NumIoThreadsProp;
import static kafka.server.KafkaConfig.NumNetworkThreadsProp;
import static kafka.server.KafkaConfig.NumPartitionsProp;
import static kafka.server.KafkaConfig.NumRecoveryThreadsPerDataDirProp;
import static kafka.server.KafkaConfig.OffsetsTopicReplicationFactorProp;
import static kafka.server.KafkaConfig.PortProp;
import static kafka.server.KafkaConfig.SocketReceiveBufferBytesProp;
import static kafka.server.KafkaConfig.SocketRequestMaxBytesProp;
import static kafka.server.KafkaConfig.SocketSendBufferBytesProp;
import static kafka.server.KafkaConfig.ZkConnectProp;
import static kafka.server.KafkaConfig.ZkConnectionTimeoutMsProp;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static scala.collection.JavaConverters.asScalaBuffer;

import java.util.ArrayList;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.TemporaryFolder;
import scala.Option;

public class EmbeddedKafka {

  private static final int INIT_PORT = 9094;

  private final TemporaryFolder temporaryFolder;

  private final Integer replicationFactor;

  private final String zkConnect;

  private KafkaServer kafkaServer;

  private EmbeddedKafka(
      TemporaryFolder temporaryFolder, Integer replicationFactor, String zkConnect) {
    this.temporaryFolder = temporaryFolder;
    this.replicationFactor = replicationFactor;
    this.zkConnect = zkConnect;
  }

  public static EmbeddedKafka builder(
      TemporaryFolder temporaryFolder, Integer replicationFactor, String zkConnect) {
    return new EmbeddedKafka(temporaryFolder, replicationFactor, zkConnect);
  }

  public EmbeddedKafka create(Integer nodeId) throws Exception {
    int port = INIT_PORT + nodeId;
    String logDir = temporaryFolder.newFolder().getAbsolutePath();

    KafkaConfig kafkaConfig = getKafkaConfig(nodeId, port, logDir);

    kafkaServer =
        new KafkaServer(kafkaConfig, Time.SYSTEM, Option.empty(), asScalaBuffer(new ArrayList<>()));
    kafkaServer.startup();
    return this;
  }

  public void start() {
    if (kafkaServer.brokerState().currentState() == (NotRunning.state())) {
      kafkaServer.startup();
    }
  }

  public void shutdown() {
    if (kafkaServer.brokerState().currentState() != (NotRunning.state())) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }
  }

  public String getKafkaConnect() {
    return "localhost:" + kafkaServer.config().port().toString();
  }

  private KafkaConfig getKafkaConfig(Integer nodeId, Integer port, String logDir) {
    Properties configProperties = new Properties();
    configProperties.setProperty(BrokerIdProp(), nodeId.toString());
    configProperties.put(PortProp(), port);
    configProperties.put(ListenersProp(), String.format("%s://localhost:%s", PLAINTEXT, port));
    configProperties.setProperty(NumNetworkThreadsProp(), "3");
    configProperties.setProperty(NumIoThreadsProp(), "8");
    configProperties.setProperty(SocketSendBufferBytesProp(), "102400");
    configProperties.setProperty(SocketReceiveBufferBytesProp(), "102400");
    configProperties.setProperty(SocketRequestMaxBytesProp(), "104857600");
    configProperties.setProperty(LogDirProp(), logDir);
    configProperties.setProperty(NumPartitionsProp(), "1");
    configProperties.setProperty(NumRecoveryThreadsPerDataDirProp(), "1");
    configProperties.setProperty(OffsetsTopicReplicationFactorProp(), replicationFactor.toString());
    configProperties.setProperty(LogRetentionTimeHoursProp(), "168");
    configProperties.setProperty(LogSegmentBytesProp(), "1073741824");
    configProperties.setProperty(LogCleanupIntervalMsProp(), "300000");
    configProperties.setProperty(ZkConnectProp(), zkConnect);
    configProperties.setProperty(ZkConnectionTimeoutMsProp(), "6000");
    configProperties.setProperty(AutoLeaderRebalanceEnableProp(), "true");
    configProperties.setProperty(DeleteTopicEnableProp(), "true");

    return new KafkaConfig(configProperties);
  }
}
