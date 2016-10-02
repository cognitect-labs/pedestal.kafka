(ns io.pedestal.kafka.consumer
  (:require [clojure.spec :as s]
            [io.pedestal.kafka.common :as common])
  (:import [org.apache.kafka.common.serialization Deserializer]
           [org.apache.kafka.clients.consumer ConsumerInterceptor]))

(s/def ::key.deserializer               #(instance? Deserializer %))
(s/def ::value.deserializer             #(instance? Deserializer %))

(s/def ::auto.commit.interval.ms        ::common/time)
(s/def ::auto.offset.reset              string?)
(s/def ::check.crcs                     boolean?)
(s/def ::enable.auto.commit             boolean?)
(s/def ::exclude.internal.topics        boolean?)
(s/def ::fetch.max.wait.ms              ::common/time)
(s/def ::fetch.min.bytes                ::common/size)
(s/def ::group.id                       string?)
(s/def ::heartbeat.interval.ms          ::common/time)
(s/def ::interceptor.classes            #(instance? ConsumerInterceptor %))
(s/def ::max.partition.fetch.bytes      ::common/size)
(s/def ::max.poll.records               ::common/size)
(s/def ::partition.assignment.strategy  string?)
(s/def ::session.timeout.ms             ::common/time)

(s/def ::configuration (s/keys :req [::common/bootstrap.servers
                                     ::key.deserializer
                                     ::value.deserializer]
                               :opt [::auto.commit.interval.ms
                                     ::auto.offset.reset
                                     ::check.crcs
                                     ::enable.auto.commit
                                     ::exclude.internal.topics
                                     ::fetch.max.wait.ms
                                     ::fetch.min.bytes
                                     ::group.id
                                     ::heartbeat.interval.ms
                                     ::interceptor.classes
                                     ::max.partition.fetch.bytes
                                     ::max.poll.records

                                     ::partition.assignment.strategy
                                     ::session.timeout.ms

                                     ::common/client.id
                                     ::common/connections.max.idle.ms
                                     ::common/metadata.max.age.ms
                                     ::common/receive.buffer.bytes
                                     ::common/reconnect.backoff.ms
                                     ::common/request.timeout.ms
                                     ::common/retry.backoff.ms
                                     ::common/security.protocol
                                     ::common/send.buffer.bytes

                                     ::common/metric.reporters
                                     ::common/metrics.num.samples
                                     ::common/metrics.sample.window.ms

                                     ::common/ssl.key.password
                                     ::common/ssl.keystore.location
                                     ::common/ssl.keystore.password
                                     ::common/ssl.truststore.location
                                     ::common/ssl.truststore.password
                                     ::common/ssl.enabled.protocols
                                     ::common/ssl.keystore.type
                                     ::common/ssl.protocol
                                     ::common/ssl.provider
                                     ::common/ssl.truststore.type
                                     ::common/ssl.cipher.suites
                                     ::common/ssl.endpoint.identification.algorithm
                                     ::common/ssl.keymanager.algorithm
                                     ::common/ssl.trustmanager.algorithm

                                     ::common/sasl.kerberos.service.name
                                     ::common/sasl.mechanism
                                     ::common/sasl.kerberos.kinit.cmd
                                     ::common/sasl.kerberos.min.time.before.relogin
                                     ::common/sasl.kerberos.ticket.renew.jitter
                                     ::common/sasl.kerberos.ticker.renew.window.factor
                                     ]))
