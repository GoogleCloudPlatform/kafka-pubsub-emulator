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

# Script to configure all necessary containers for running the PubSub emulator benchmark

set -eu -o pipefail
NETWORK=pubsub-emulator-benchmarking

start_containers() {
    # Starts a Kafka cluster on the local Docker host
    local host_ip=$(docker network inspect bridge | grep Gateway | cut -d: -f 2 | tr -d ' "')
    echo "==== Starting Kafka cluster on docker ===="

    echo "Creating Docker network ${NETWORK}"
    docker network create ${NETWORK}

    echo "Starting zookeeper"
    docker run -d -h zookeeper --name zookeeper \
        --network ${NETWORK} \
        -p 2181 \
        --label pubsub-emulator-benchmark \
        wurstmeister/zookeeper

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
            --env KAFKA_LISTENERS=PLAINTEXT://:9092 \
            --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-${i}:9092 \
            --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
            --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
            --env KAFKA_AUTO_LEADER_REBALANCE_ENABLE='true' \
            --env KAFKA_AUTO_CREATE_TOPICS_ENABLE='false' \
            --env KAFKA_LOG_RETENTION_MS=30000 \
            ${topics} \
            --network ${NETWORK} \
            -p 9092 \
            --label pubsub-emulator-benchmark \
            --volume kafka-${i}:/kafka \
            wurstmeister/kafka
    done

    i=0
    while (( i < 20 )); do
        sleep 1
        echo "Waiting for $(( 20 - i ))s"
        (( i += 1 ))
    done

    echo "Starting Pub/Sub emulator"
    gcloud docker -- pull us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator:1.0.0.0
    docker run -d -h kafka-pubsub-emulator --name kafka-pubsub-emulator \
        --mount type=bind,src=$(pwd)/config,dst=/etc/config,ro=true \
        -p 8080:8080 \
        --network ${NETWORK} \
        --label pubsub-emulator-benchmark \
        us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator:1.0.0.0 \
        --configuration.location=/etc/config/application.yaml

    echo "Running containers..."
    docker ps --filter "label=pubsub-emulator-benchmark"
}

stop_containers() {
    # Stops and removes all artifacts from the local Docker host
    echo "==== Stopping and cleaning up Kafka cluster on docker ===="
    docker container rm -f zookeeper kafka-0 kafka-1 kafka-2 kafka-pubsub-emulator
    docker volume rm -f kafka-0 kafka-1 kafka-2
    docker network rm ${NETWORK}
}

if [[ "${1:-}" == "start" ]] ; then
    start_containers
else
    stop_containers
fi
