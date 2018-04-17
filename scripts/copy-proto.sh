#!/usr/bin/env bash

find target/protoc-dependencies -type f -name "pubsub.proto" -exec cp {} go/src/kafka-pubsub-emulator-gateway/proto \;

cp src/main/proto/admin.proto go/src/kafka-pubsub-emulator-gateway/proto
