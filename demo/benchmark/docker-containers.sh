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

# Script to configure all necessary containers for running the PubSub emulator benchmark. This
# should after changing to this directory.

set -eu -o pipefail
NETWORK_LABEL=kafka-testing
DOCKER_SCRIPT="../../docker-kafka.sh"

build_and_start_emulator() {
    local version=$(grep --max-count=1 '<version>' ../../pom.xml | awk -F '>' '{ print $2 }' | \
        awk -F '<' '{ print $1 }')
    echo "Building Pub/Sub emulator Docker image - $version"
    docker build --build-arg version=${version} -t kafka-pubsub-emulator:${version} ../../

    echo "Starting Pub/Sub emulator exposed at localhost:8080"
    docker run -d -h kafka-pubsub-emulator --name kafka-pubsub-emulator \
        --mount type=bind,src=$(pwd)/config,dst=/etc/config,ro=true \
        -p 8080:8080 \
        --network "$NETWORK_LABEL" \
        --label "$NETWORK_LABEL" \
        kafka-pubsub-emulator:${version} \
        -c /etc/config/config.json -p /etc/config/pubsub.json

    echo "Running containers..."
    docker ps --filter "label=$NETWORK_LABEL"
}

if [[ "${1:-}" == "start" ]] ; then
    "$DOCKER_SCRIPT" start \
      "performance-testing-8p:8:2,performance-testing-32p:32:2,performance-testing-64p:64:2,performance-testing-128p:128:2"

    i=0
    while (( i < 20 )); do
        sleep 1
        echo "Waiting for $(( 20 - i ))s"
        (( i += 1 ))
    done
    build_and_start_emulator
else
    docker container rm -f kafka-pubsub-emulator
    "$DOCKER_SCRIPT" stop
fi
