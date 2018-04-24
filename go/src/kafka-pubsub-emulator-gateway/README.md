# Pub/Sub Emulator Gateway for Kafka

This project implements a gRPC gateway server based on [gRPC Gateway](https://github.com/grpc-ecosystem/grpc-gateway), 
The gateway is exposed as a standalone [Go Lang](https://golang.org/) application with mandatory configuration of address to connect on Pub/Sub Emulator for Kafka 
passed as an argument at runtime, creating gateway a server with REST endpoints of Pub/Sub services.

## Building and Running
First, ensure that you have installed the 
[Google Protocol Buffer compiler](https://github.com/google/protobuf) (protoc) for your environment,
and that it is available in your PATH environment variable.

You will also need to install the latest stable version of [Go](https://golang.org/project/). Then, 
follow the [instructions](https://github.com/grpc-ecosystem/grpc-gateway#installation) from the 
gRPC Gateway project to configure your environment.

Then, install the additional Golang dependencies.
```bash
go get -u github.com/spf13/cobra
go get -u google.golang.org/grpc/credentials
```

Checkout the source and then build executing the `./build.sh install`. This will generate a binary content of application at `./src/kafka-pubsub-emulator-gateway/cmd`.

### Standalone Application
To start the application, you must have the Pub/Sub Emulator for Kafka running on your machine and 
specify the host and port using `-a` or `--address` command line argument.

```
./cmd/kafka-pubsub-emulator-gateway start -a localhost:8080
```

Pub/Sub Emulator Gateway for Kafka will run by default on port 8181 but you can change. More 
details can be found in the [Configuration Arguments](#configuration-arguments) section below.

### Docker

This configuration assumes that you the built the project using the 
[Building and Running](#building-and-running) steps above.

```
docker build -t kafka-pubsub-emulator-gateway:1.0.0.0 .
docker run -p 8181:8181 kafka-pubsub-emulator-gateway:1.0.0.0 -a localhost:8080
```

If you want to build project and build the container execute `./build.sh` after finish build process execute `docker run -p 8181:8181 kafka-pubsub-emulator-gateway:1.0.0.0 -a localhost:8080` to launch the application.

### Kubernetes

The configuration for Kubernetes was based on Minikube. To configure see more 
[here](https://kubernetes.io/docs/tutorials/stateless-application/hello-minikube/).

This example uses the default configuration from resources folders, it assumes that you have a 
Minikube instance of a Pub/Sub Kafka Emulator running on port 30080. 
(If you are running locally change the address port of 
kubernetes/kafka-pubsub-emulator-gateway-deployment.yaml file)

Build kafka pubsub emulator gateway container:
```
cd go/src/kafka-pubsub-emulator-gateway
docker build -t kafka-pubsub-emulator-gateway:1.0.0.0 .
```

Create deployment of kafka-emulator-gateway [see more](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), with 1 application pods. 
```
export PUBSUB_EMULATOR_MINIKUBE_IP=$(minikube ip)
kubectl create configmap emulator-info --from-literal ip=$PUBSUB_EMULATOR_MINIKUBE_IP
kubectl create -f kubernetes/kafka-pubsub-emulator-gateway-deployment.yaml
```

Create service load balancer for kafka-emulator-gateway [see more](https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/). 
```
kubectl create -f kubernetes/kafka-pubsub-emulator-gateway-loadbalancer.yaml
```

### Configuration Arguments
The Pub/Sub Emulator Gateway server needs to be started with a `start` command and address flag 
to connect on Pub/Sub Emulator for Kafka.

#### Required Flag
- **-a** or **--address**: This is the address to point the REST gateway to pub/sub Emulator 
for Kafka (host:port).

#### Optional Flag
- **-p** or **--port**: Specifies the application port default value is 8181.
- **-c** or **--cert_file_path**: Path to the certificate chain file to connect on Pub/Sub Emulator 
  for Kafka.
- **-h** or **--help**: Help describe arguments 

## Endpoints

* **Health endpoints**
  * Provide status information of services more information 
    [here](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
    * **GET** *http://localhost:8181/v1/health?service=<name_of_service>*
    * **GET** *http://localhost:8181/swagger/health.swagger.json*
    
* **Admin endpoints**
  * Provide admin information about statistics and configuration on Pub/Sub Emulator Kafka.
    * **GET** *http://localhost:8181/v1/admin/statistics*
    * **GET** *http://localhost:8181/v1/admin/configuration*
    * **GET** *http://localhost:8181/swagger/admin.swagger.json*

* **Pub/Sub endpoints**
 * Provide Pub/Sub REST endpoints if you need a detailed information by each endpoint 
   [here](https://cloud.google.com/pubsub/docs/reference/rest/).
   * **GET** *http://localhost:8181/swagger/pubsub.swagger.json*