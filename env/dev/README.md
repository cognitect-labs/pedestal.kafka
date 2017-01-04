# Development Environment

## One-time Setup

### Linux

Install docker version 1.12 or later.

## Startup

Start all the containers at once:

    bin/up

## Kafka Smoke Test

Before going any further, make sure that Kafka is running correctly
and it can reach Zookeeper.

    cd $YOUR_KAFKA_DOWNLOAD

    # Create a topic
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic smoketest

    # Check that the topic was created
    bin/kafka-topics.sh --list   --zookeeper localhost:2181

    # Put a message onto the topic
    echo "a test message" | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic smoketest

    # Read the message from the topic
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic smoketest --from-beginning

You should see "a test message" as the output. Hit Ctrl-C to stop the
console consumer.

## Shutdown

Just hit Ctrl-C in the terminal window where you see the logs.

## Checking Kafka topics

    cd $YOUR_KAFKA_DOWNLOAD
    bin/kafka-topics.sh --list --zookeeper localhost:2181
