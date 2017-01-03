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

TBD

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

Copyright © 2016 Cognitect, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
