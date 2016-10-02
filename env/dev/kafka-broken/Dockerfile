FROM java:8-jre

MAINTAINER Michael Nygard (http://gitlab.com/mtnygard)

ENV KAFKA_VERSION 0.10.0.1
ENV SCALA_VERSION 2.11

RUN wget -q -O - "http://apache.mirrors.pair.com/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" | tar -xzf - -C /opt \
    && mv /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION /opt/kafka

ENV PATH /opt/kafka/bin:$PATH

COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

COPY ./image/conf /opt/kafka/config
VOLUME ["/opt/kafka/config"]

EXPOSE 9092
EXPOSE 15000

CMD  ["kafka-server-start.sh", "/opt/kafka/config/server.properties"]
