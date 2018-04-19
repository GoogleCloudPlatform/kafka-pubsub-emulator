#!/usr/bin/env bash
#  Copyright 2018 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Script to help start stop local Kafka cluster on docker

set -eu -o pipefail

EMULATOR_PID=0

trap cleanup 1 2 3 6

start_docker_kafka() {
    # Starts a Kafka cluster on the local Docker host
    local host_ip=$(docker network inspect bridge | grep Gateway | cut -d: -f 2 | tr -d ' "')
    echo "==== Starting Kafka cluster on docker ===="

    echo "Creating Docker network emulator-benchmarking"
    docker network create emulator-benchmarking

    echo "Starting zookeeper"
    docker run -d -h zookeeper --name zookeeper \
        -p 2181:2181 \
        --network emulator-benchmarking \
        wurstmeister/zookeeper

    local port=9094
    for i in 0 1 2
    do
        topics=
        if [[ ${i} -eq 0 ]]
        then
            topics="--env KAFKA_CREATE_TOPICS=performance-testing-8p:8:2,performance-testing-32p:32:2,performance-testing-64p:64:2,performance-testing-128p:128:2"
        fi

        echo "Starting broker kafka-${i}"
        docker volume create kafka-${i}
        docker run -d -h kafka-${i} --name kafka-${i} \
            --env KAFKA_BROKER_ID=${i} \
            --env KAFKA_ADVERTISED_HOST_NAME=${host_ip} \
            --env KAFKA_ADVERTISED_PORT=${port} \
            --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
            --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
            --env KAFKA_AUTO_LEADER_REBALANCE_ENABLE='true' \
            --env KAFKA_AUTO_CREATE_TOPICS_ENABLE='false' \
            --env KAFKA_LOG_RETENTION_MS=30000 \
            ${topics} \
            -p ${port}:9092 \
            --network emulator-benchmarking \
            --volume kafka-${i}:/kafka \
            wurstmeister/kafka

        (( port++ ))
    done
}

stop_docker_kafka() {
    # Stops and removes all artifacts from the local Docker host
    echo "==== Stopping and cleaning up Kafka cluster on docker ===="
    docker rm -f zookeeper kafka-0 kafka-1 kafka-2
    docker volume rm -f kafka-0 kafka-1 kafka-2
    docker network rm emulator-benchmarking
}

start_emulator() {
    echo "==== Starting PubsubEmulator Server on port 8080 ===="
    java -Djava.util.logging.config.file=./benchmarking/logging.properties \
      -cp "./benchmarking:./target/kafka-pubsub-emulator-1.0.0.0.jar" \
      com.google.cloud.partners.pubsub.kafka.PubsubEmulatorServer \
      --configuration.location=benchmarking/application-benchmarking.yaml > benchmarking/emulator.stdout 2>&1 &
    EMULATOR_PID=$!
}

cleanup() {
    echo "==== Stopping PubsubEmulator Server and tearing down Kafka containers ===="
    [[ $EMULATOR_PID -gt 0 ]] && kill $EMULATOR_PID
    stop_docker_kafka
}

main() {
    start_docker_kafka
    mvn package
    start_emulator
    if [[ "${1:-}" == "server" ]] ; then
        echo "==== Waiting for Crtl-C to Kill $EMULATOR_PID ===="
        wait $EMULATOR_PID
    else
        mvn -Dtest=PerformanceBenchmarkIT test | tee benchmarking/performance_tests.log
        cleanup
    fi

    return 0
}

main $*
