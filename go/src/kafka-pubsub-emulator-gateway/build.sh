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

set -eu -o pipefail

PROTO_PATH=proto/*.proto
GOPATH=$(go env GOPATH)

clean() {
  echo "==== Initiating Clean Process ===="
  # clean cmd, internal
  rm -rf internal/*
  rm -rf cmd/*
  mkdir internal/swagger
  echo "==== Finish Clean Process ===="
}

generate_proto_stubs(){
  for filename in ${PROTO_PATH}; do
    protoc -I/usr/local/include -I. \
      -I${GOPATH}/src \
      -I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
      --plugin=protoc-gen-go=${GOPATH}/bin/protoc-gen-go \
      --go_out=plugins=grpc:./internal \
      ${filename}
  done
}

generate_reverse_proxy(){
  for filename in ${PROTO_PATH}; do
    protoc -I/usr/local/include -I. \
      -I${GOPATH}/src/google.golang.org/genproto/googleapis \
      -I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
      -I${GOPATH}/src/github.com/golang/protobuf/ptypes \
      -I${GOPATH}/src \
      --plugin=protoc-gen-grpc-gateway=${GOPATH}/bin/protoc-gen-grpc-gateway \
      --grpc-gateway_out=logtostderr=true:./internal \
      ${filename}
   done
}

generate_swagger_json() {
 for filename in ${PROTO_PATH}; do
   protoc -I/usr/local/include -I. \
     -I${GOPATH}/src \
     -I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
     --plugin=protoc-gen-swagger=${GOPATH}/bin/protoc-gen-swagger \
     --swagger_out=logtostderr=true:./internal/swagger \
     ${filename}
 done
}

build() {
  echo "==== Initiating Build Process ===="
  generate_proto_stubs
  generate_reverse_proxy
  generate_swagger_json

  CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cmd/kafka-pubsub-emulator-gateway .

  docker build -t kafka-pubsub-emulator-gateway:1.0.0.0 .
  echo "==== Finish Build Process ===="
}

execute() {
  clean
  build
}

if [[ "${1:-}" == "clean" ]] ; then
  clean
elif [[ "${1:-}" == "build" ]] ; then
  build
elif [[ "${1:-}" == "install" ]] ; then
  CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cmd/kafka-pubsub-emulator-gateway .
else
  execute
fi
