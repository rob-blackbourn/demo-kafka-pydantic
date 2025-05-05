# demo-kafka-pydantic

A demo of using kafka with [pydantic](https://github.com/pydantic/pydantic) using the clients:

* [kafka-python](https://github.com/dpkp/kafka-python) - kafka_ex1
* [aiokafka](https://github.com/aio-libs/aiokafka) - kafka_ex2
* [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) - kafka_ex3

These require Python 3.12 or greater.

## Running Kafka in a Container

The instructions for the image are [here](https://hub.docker.com/r/apache/kafka).

### Generic docker instructions

Run the latest kafka, exposing the port 9092.

```bash
docker run -d -p 9092:9092 --name kafka-broker apache/kafka:latest
```

When you are finished, stop and remove the container by running the following command on your host machine:

```bash
docker rm -f kafka-broker
```

### What I Did

I was using podman on a Mac M4. This required the `_JAVA_OPTIONS` setting.

Run kafka.

```bash
podman run -d -e _JAVA_OPTIONS="-XX:UseSVE=0" -p 9092:9092 --name kafka-broker apache/kafka:latest
```

When you are finished, stop and remove the container by running the following command on your host machine:

```bash
podman rm -f kafka-broker
```
