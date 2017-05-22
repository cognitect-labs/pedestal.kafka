[![CircleCI](https://circleci.com/gh/cognitect-labs/pedestal.kafka.svg?style=svg&circle-token=26a5199f9dc64fa3265a576a968053410bf4399e)](https://circleci.com/gh/cognitect-labs/pedestal.kafka)

# pedestal.kafka

Pedestal interceptors and chain provider for Kafka applications.

A Pedestal
[chain provider](https://io.pedestal/reference/chain-provider)
connects a network library or container to an application via
interceptors. The chain provider creates the execution context and
invokes a stack of interceptors.

This library provides interceptors that can parse and process Kafka
messages. Other interceptors are provided to generate new Kafka
messages and topics.

## Usage

### Consuming Topics

One service map handles one topic. If you need to handle multiple
topics, just make several service maps and start each one.

### Define a Configuration

A "server" starts with a configuration that tells the Kafka client
library how to connect. The configuration is a map with specific key
names.

Configurations are fully specified in `com.cognitect.kafka.consumer`
and `com.cognitect.kafka.producer`. Both of these pull in specs from
`com.cognitect.kafka.common`.

All the settings come directly from Kafka's client libraries. See
[Kafka Documentation](https://kafka.apache.org/documentation.html#configuration)
for details.

### Attach Interceptors

Attach interceptors to your service map by assoc-ing them to the key
`com.cognitect.kafka.consumer/interceptors`.

Each interceptor's `:enter` function will be called with a context map
that includes the key `:message`. The first interceptor's message
value will be a Kafka
[ConsumerRecord](https://kafka.apache.org/0102/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerRecord.html).

After that it depends on how your interceptors transform the message.

### Create a server and start it

Pass the configuration to `com.cognitect.kafka/kafka-server`. That
transforms the configuration map and returns a "service map".

Start the service map by passing it to
`com.cognitect.kafka/start`. That returns the "server". You'll want to
keep a reference to that server so you can stop it later.

## Built-in Interceptors

This library provides some interceptors that help process messages.

- `com.cognitect.kafka.parser/edn-value`
- `com.cognitect.kafka.parser/json-value`
- `com.cognitect.kafka.parser/transit-json-value`
- `com.cognitect.kafka.parser/transit-msgpack-value`

## Style Advice

The whole point of interceptors is to break a large computation or
data transformation into manageable, isolated chunks. Think about how
you can reduce semantic dependencies by communicating through the context.

# Developing Pedestal.kafka

The easiest way to set up is with
[docker-compose](https://docs.docker.com/compose/) and the env
directory.

```
$ cd env/dev
$ bin/up
```

Several examples in `comment` sections use the Kafka broker and
Zookeeper nodes created by the docker-compose file. You can, of
course, replace hostnames and ports to connect to any other Kafka and
Zookeeper instances.

## License

Copyright © 2016-2017 Cognitect, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
