# Preparing for running Demos

## Starting Apache Kafka in Docker

If you already have access to an existing Apache Kafka cluster, you can skip this section. 
If not, the project contains a shell script for starting a basic Apache Kafka configuration
running locally in Docker. You can download Docker for your operating system from the link below:

https://www.docker.com/community-edition

Start the Apache Kafka cluster by running issuing the following command from the root of this 
repository:

`./docker-kafka.sh start`

Your output should look similar to what’s shown below.
```
==== Starting Kafka cluster on docker ====
Creating Docker network kafka-testing
55689ea783b3b9bd2d497c6f4fa17b9e262e214f70575a270c737b292c1f9d54
Starting zookeeper
a5721431e8a68fb2ffb2c3f1f1c03087f3cedf46749c12a5534c7f73c5085e21
Starting broker kafka-0
a6812c5521e3b8c595ccbe61990a2048e822cd7be755a2f7e5fd5dde6b9939ed
Starting broker kafka-1
62865bd62c9914e31f65752819689fdf59e5b78f750f67930f168c273f362e3b
Starting broker kafka-2
Ff368a8aa2aeed90d6771a056e12385158330bc1b7fce9fad37ef899d7fcb906
```

Issuing `docker container ls` should show you the running Docker instances and their ports.
```
~/tmp/kafka-pubsub-emulator$ docker container ls
CONTAINER ID        IMAGE                    COMMAND                  CREATED             STATUS              PORTS                                                NAMES
ff368a8aa2ae        wurstmeister/kafka       "start-kafka.sh"         52 seconds ago      Up 51 seconds       0.0.0.0:9096->9092/tcp                               kafka-2
62865bd62c99        wurstmeister/kafka       "start-kafka.sh"         54 seconds ago      Up 52 seconds       0.0.0.0:9095->9092/tcp                               kafka-1
a6812c5521e3        wurstmeister/kafka       "start-kafka.sh"         55 seconds ago      Up 54 seconds       0.0.0.0:9094->9092/tcp                               kafka-0
a5721431e8a6        wurstmeister/zookeeper   "/bin/sh -c '/usr/..."   55 seconds ago      Up 55 seconds       22/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp   zookeeper
```

## Building the Emulator Package
If you want to build the emulator from source and run it locally, ensure that you have 
[Maven](https://maven.apache.org/) installed and issue `mvn package` command from the root 
directory.

The build should complete in a few minutes and you should have a single JAR file inside of the 
`target/` directory that contains the executable binary for the emulator.

## Running the Emulator in Docker
There are different options for running the emulator, but all of them require configuration
files that provide details on what Topics and Subscriptions the emulator is able to support, 
as well as some additional settings that affect how the emulator behaves when communicating with 
Kafka. More details can be found in the [README](../README.md) file within the main repository.

For an example of a working configuration with the emulator running on Docker, see the [benchmark](./benchmark)
directory.

## Running the Emulator REST Gateway in Docker
If you want to run the [python-rest-client](./python-rest-client) demo, you'll also need to start up
an instance of the PubSub Emulator Gateway, which exposes the Pub/Sub API through a REST interface.

```bash
gcloud docker -- pull us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator-gateway:<version>
docker run -d -h kafka-pubsub-emulator-gateway --name kafka-pubsub-emulator-gateway \
    -p 8181:8181 \
    --network kafka-testing \
    us.gcr.io/kafka-pubsub-emulator/kafka-pubsub-emulator-gateway:<version> \
    -a kafka-pubsub-emulator:8080
```

If everything is running correctly, you should be able to list Topics or Subscriptions by issuing a
simple cURL request.
```bash
curl http://localhost:8181/v1/projects/any/subscriptions
curl http://localhost:8181/v1/projects/any/topics
```