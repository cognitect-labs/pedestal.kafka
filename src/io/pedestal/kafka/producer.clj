(ns io.pedestal.kafka.producer
  (:require [clojure.spec :as s]
            [io.pedestal.kafka.common :as common])
  (:import [org.apache.kafka.common.serialization Serializer]
           [org.apache.kafka.clients.producer Partitioner ProducerInterceptor]))

(s/def ::key.serializer                           #(instance? Serializer %))
(s/def ::value.serializer                         #(instance? Serializer %))

(s/def ::acks                                     #{:all -1 0 1})
(s/def ::buffer.memory                            ::common/size)
(s/def ::compression.type                         string?)
(s/def ::retries                                  integer?)
(s/def ::batch.size                               ::common/size)
(s/def ::linger.ms                                ::common/time)
(s/def ::max.block.ms                             ::common/time)
(s/def ::max.request.size                         ::common/size)
(s/def ::partitioner.class                        #(instance? Partitioner %))
(s/def ::timeout.ms                               ::common/time)
(s/def ::block.on.buffer.full                     boolean?)
(s/def ::max.in.flight.requests.per.connection    ::common/size)
(s/def ::metadata.fetch.timeout.ms                ::common/time)
(s/def ::interceptor.classes                      (s/coll-of #(instance? ProducerInterceptor %)))

(s/def ::configuration (s/keys :req [::common/bootstrap.servers
                                     ::key.serializer
                                     ::value.serializer]
                               :opt [::acks
                                     ::buffer.memory
                                     ::compression.type
                                     ::retries
                                     ::interceptor.classes
                                     ::batch.size
                                     ::linger.ms
                                     ::max.block.ms
                                     ::max.request.size
                                     ::partitioner.class
                                     ::timeout.ms
                                     ::block.on.buffer.full
                                     ::max.in.flight.requests.per.connection
                                     ::metadata.fetch.timeout.ms
                                     ::common/metadata.max.age.ms
                                     ::common/client.id
                                     ::common/connections.max.idle.ms
                                     ::common/receive.buffer.bytes
                                     ::common/request.timeout.ms
                                     ::common/security.protocol
                                     ::common/send.buffer.bytes
                                     ::common/metric.reporters
                                     ::common/metrics.num.samples
                                     ::common/metrics.sample.window.ms
                                     ::common/reconnect.backoff.ms
                                     ::common/retry.backoff.ms
                                     ::common/sasl.kerberos.service.name
                                     ::common/sasl.mechanism
                                     ::common/sasl.kerberos.kinit.cmd
                                     ::common/sasl.kerberos.min.time.before.relogin
                                     ::common/sasl.kerberos.ticket.renew.jitter
                                     ::common/sasl.kerberos.ticker.renew.window.factor
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
                                     ::common/ssl.trustmanager.algorithm]))
