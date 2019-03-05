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
#  Note: This Dockerfile assumes that you have run mvn package to build the JAR inside of target.
FROM openjdk:8-alpine

ARG version
ENV port=8080

# Set the working directory to /app
WORKDIR /app

# Copy the target directory contents into the container at /app
COPY ./target/kafka-pubsub-emulator-$version.jar /app/kafka-pubsub-emulator.jar
COPY ./src/main/resources/logging.properties /app

# Make port 8080 available to the world outside this container
EXPOSE $port

# Run the main application JAR at launch
ENTRYPOINT ["java", "-Djava.util.logging.config.file=logging.properties", "-jar", "kafka-pubsub-emulator.jar"]
CMD ["--help"]