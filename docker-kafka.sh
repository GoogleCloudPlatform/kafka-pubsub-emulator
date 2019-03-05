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

# Script to help start and stop a local Kafka cluster on docker

set -eu -o pipefail
NETWORK_LABEL=kafka-testing

start_docker_kafka() {
    # Starts a Kafka cluster on the local Docker host
    # Args:
    #   1) Comma-seperated list of topics to create in the form:
    #     <topic name>:<partitions>:<replication factor>
    local host_ip=$(docker network inspect bridge | grep Gateway | cut -d: -f 2 | tr -d ' "')
    echo "==== Starting Kafka cluster on docker ===="

    echo "Creating Docker network $NETWORK_LABEL"
    docker network create "$NETWORK_LABEL"

    echo "Starting zookeeper"
    docker run -d -h zookeeper --name zookeeper \
        --network "$NETWORK_LABEL" \
        -p 2181:2181 \
        --label "$NETWORK_LABEL" \
        wurstmeister/zookeeper

    local port=9094
    for i in 0 1 2
    do
        topics=
        if [[ ${i} -eq 1 ]]
        then
            topics="--env KAFKA_CREATE_TOPICS=$1"
            echo "$topics"
        fi

        echo "Starting broker kafka-${i}"
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
            -p ${port}:9092 \
            --label "$NETWORK_LABEL" \
            --network "$NETWORK_LABEL" \
            --volume kafka-${i}:/kafka \
            wurstmeister/kafka

        (( port++ ))
    done

    echo "Running containers..."
    docker ps --filter "label=$NETWORK_LABEL"
}

stop_docker_kafka() {
    # Stops and removes all artifacts from the local Docker host
    echo "==== Stopping and cleaning up Kafka cluster on docker ===="
    docker rm -f zookeeper kafka-0 kafka-1 kafka-2
    docker volume rm -f kafka-0 kafka-1 kafka-2
    docker network rm "$NETWORK_LABEL"
}

# When starting, you must provide a comma-seperated list of Kafka topics to create
if [[ "${1:-}" == "start" ]] ; then
    start_docker_kafka "$2"
else
    stop_docker_kafka
fi