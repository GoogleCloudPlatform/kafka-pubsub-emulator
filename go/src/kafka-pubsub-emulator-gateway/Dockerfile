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
FROM alpine:3.7

WORKDIR /app

COPY internal/swagger /app/internal/swagger
COPY cmd/kafka-pubsub-emulator-gateway /app/kafka-pubsub-emulator-gateway

ENTRYPOINT ["/app/kafka-pubsub-emulator-gateway","start"]
CMD ["-h"]