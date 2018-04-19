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

start_docker_kafka() {
    # Starts a Kafka cluster on the local Docker host
    local host_ip=$(docker network inspect bridge | grep Gateway | cut -d: -f 2 | tr -d ' "')
    echo "==== Starting Kafka cluster on docker ===="

    echo "Creating Docker network kafka-testing"
    docker network create kafka-testing

    echo "Starting zookeeper"
    docker run -d -h zookeeper --name zookeeper \
        -p 2181:2181 \
        --network kafka-testing \
        wurstmeister/zookeeper

    local port=9094
    for i in 0 1 2
    do
        topics=
        if [[ ${i} -eq 1 ]]
        then
            topics="--env KAFKA_CREATE_TOPICS=throughput-testing-3p:3:2,throughput-testing-6p:6:2,throughput-testing-100p:100:2,testing-10p:10:2"
        fi

        echo "Starting broker kafka-${i}"
        docker run -d -h kafka-${i} --name kafka-${i} \
            --env KAFKA_BROKER_ID=${i} \
            --env KAFKA_ADVERTISED_HOST_NAME=${host_ip} \
            --env KAFKA_ADVERTISED_PORT=${port} \
            --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
            --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
            --env KAFKA_AUTO_LEADER_REBALANCE_ENABLE='true' \
            ${topics} \
            -p ${port}:9092 \
            --network kafka-testing \
            wurstmeister/kafka

        (( port++ ))
    done
}

stop_docker_kafka() {
    # Stops and removes all artifacts from the local Docker host
    echo "==== Stopping and cleaning up Kafka cluster on docker ===="
    docker rm -f zookeeper kafka-0 kafka-1 kafka-2
    docker network rm kafka-testing
}

if [[ "${1:-}" == "start" ]] ; then
    start_docker_kafka
else
    stop_docker_kafka
fi