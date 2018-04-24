#!/usr/bin/env bash

# Upload coverage
bash <(curl -s https://codecov.io/bash)
# Upload JAR to GCS
version=$(grep --max-count=1 '<version>' pom.xml | awk -F '>' '{ print $2 }' | \
  awk -F '<' '{ print $1 }')
gsutil -h 'Cache-Control: no-cache, no-store, must-revalidate' cp -a public-read -v \
  target/kafka-pubsub-emulator-${version}.jar gs://kafka-pubsub-emulator/

# Build docker images for kafka-pubsub-emulator and kafka-pubsub-emulator-gateway
docker build --build-arg version=${version} -t \
  us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator:${version} .
docker build -t us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator-gateway:${version} \
  go/src/kafka-pubsub-emulator-gateway/

# Push images
gcloud docker -- push us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator:${version}
gcloud docker -- push us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator-gateway:${version}
