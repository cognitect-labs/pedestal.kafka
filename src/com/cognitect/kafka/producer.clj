(ns com.cognitect.kafka.producer
  (:require [clojure.spec.alpha           :as s]
            [com.cognitect.kafka.common   :as common]
            [clojure.core.async           :as async]
            [clojure.java.io              :as io]
            [clojure.set                  :as set]
            [clojure.walk                 :as walk]
            [clojure.edn                  :as edn]
            [com.cognitect.kafka.consumer :as consumer])
  (:import [org.apache.kafka.common.serialization ByteArraySerializer Serializer StringSerializer]
           [org.apache.kafka.clients.producer KafkaProducer Partitioner ProducerInterceptor]))

(s/def ::key.serializer                           (common/names-kindof? Serializer))
(s/def ::value.serializer                         (common/names-kindof? Serializer))

(s/def ::acks                                     #{:all "-1" "0" "1"})
(s/def ::buffer.memory                            ::common/size)
(s/def ::compression.type                         string?)
(s/def ::retries                                  common/int-string?)
(s/def ::batch.size                               ::common/size)
(s/def ::linger.ms                                ::common/time)
(s/def ::max.block.ms                             ::common/time)
(s/def ::max.request.size                         ::common/size)
(s/def ::partitioner.class                        (common/names-kindof? Partitioner))
(s/def ::timeout.ms                               ::common/time)
(s/def ::block.on.buffer.full                     common/bool-string?)
(s/def ::max.in.flight.requests.per.connection    ::common/size)
(s/def ::metadata.fetch.timeout.ms                ::common/time)
(s/def ::interceptor.classes                      (s/coll-of #(common/names-kindof? ProducerInterceptor)))

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

(def string-serializer       (.getName StringSerializer))
(def byte-array-serializer   (.getName ByteArraySerializer))

(defn producer
  "Return a producer that can be used for one or more topic->channel!
  mappings. The configs are documented at
  http://kafka.apache.org/documentation.html#producerconfigs

  The configuration map can have the same keys as
  org.apache.kafka.clients.producer.ProducerConfig, written as
  keywords for easier use from Clojure. For example the
  keyword :bootstrap.servers equates to the property
  \"bootstrap.servers\""
  [config]
  (-> config
      (walk/stringify-keys)
      (KafkaProducer.)))

;; ----------------------------------------
;; Mapping a channel onto a topic


;; (defn channel->topic!
;;   "Begins mapping the given channel to a Kafka topic for the
;;   producer. This runs as an asynchronous go loop.

;;   The topic name must be a String.

;;   The values coming from the channel must implement
;;   kafka.message.Message. This namespace has some useful transducers
;;   for creating those messages from ordinary Clojure maps.

;;   The calling application supplies a channel with the buffer and
;;   semantics that it requires.

;;   To stop this mapping, close the channel.

;;   Message sends are asynchronous. If you need to confirm when the send
;;   completes, you can supply a futures-chan. The futures-chan will
;;   get [message future] pairs.

;;   Returns a map with the key :metrics. It's value is an agent that
;;   counts the number of messages sent."
;;   ([producer topic channel]
;;    (channel->topic! producer topic channel (async/chan (async/dropping-buffer 1))))
;;   ([producer topic channel futures-chan]
;;    (let [message-count (agent 0)]
;;      (async/go-loop []
;;        (when-let [msg (async/<! channel)]
;;          (let [rec     (ProducerRecord. topic (::body msg))
;;                confirm (.send producer rec)]
;;            (async/>! futures-chan [msg confirm]))
;;          (send message-count inc)
;;          (recur)))
;;      {:metrics message-count})))
