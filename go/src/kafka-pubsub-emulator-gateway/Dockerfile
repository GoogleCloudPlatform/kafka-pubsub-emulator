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
#
#  Note: This Dockerfile assumes that you have binary file of kafka-pubsub-emulator-gateway.
FROM golang:1.8 as go_lang_builder

ENV PROTO_PATH=/tmp/proto/
ENV PROTO_VERSION=3.5.1

# Install protobuf
RUN apt-get update && apt-get -y install sudo && \
    apt-get -y install curl && apt-get -y install unzip && \
    curl -OL https://github.com/google/protobuf/releases/download/v${PROTO_VERSION}/protoc-${PROTO_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTO_VERSION}-linux-x86_64.zip -d ${PROTO_PATH} && \
    sudo mv ${PROTO_PATH}/bin/* /usr/local/bin/ && \
    sudo mv ${PROTO_PATH}/include/* /usr/local/include/ && \
    rm -rf /tmp/*

# Copy protofiles and main file
COPY . /app

WORKDIR /app

# Download go dependencies
RUN mkdir -p internal/swagger && ./build.sh install

FROM alpine:latest

ENV PORT=8181

WORKDIR /app

COPY --from=go_lang_builder /app/internal/swagger /internal/swagger
COPY --from=go_lang_builder /app/cmd/kafka-pubsub-emulator-gateway .

EXPOSE $PORT

ENTRYPOINT ["/app/kafka-pubsub-emulator-gateway","start"]
CMD ["-h"]