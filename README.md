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

## License

Copyright © 2016 Michael Nygard

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
