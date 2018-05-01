# Pub/Sub Emulator for Kafka Demo Program

This subfolder contains a simplistic program that you can use to test client code against a running
instance of the [Pub/Sub Emulator for Kafka](../README.md)

## Building and Running the Demo Program
From within this directory, issue `mvn package` to build the demo program's JAR file.
 
### Publishing
With the local emulator running and exposed from Docker on port 8080, you can publish messages to a 
topic with the following command, where **publish testing-10p** is the name of the topic we will be 
publishing to.

`java -jar target/pubsub-emulator-demo-1.0-SNAPSHOT.jar localhost:8080 publish testing-10p`

### Subscribing
The subscriber process works the same way. Simply issue the following command to start the 
Subscriber client.

`java -jar target/pubsub-emulator-demo-1.0-SNAPSHOT.jar localhost:8080 subscribe subscription-to-testing-10p`

You can run both programs in separate terminal windows to simulate simultaneous operations. You can
also interact with the Kafka cluster using Kafka's 
[standard command line tools](https://kafka.apache.org/downloads).
