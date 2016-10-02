#!/bin/bash
set -e

if [ "$1" = 'kafka-server-start.sh' ]; then

    sed -i -r 's|zookeeper.connect=localhost:2181|zookeeper.connect=zookeeper:2181|g' /opt/kafka/config/server.properties

    exec "$@"
fi

exec "$@"
