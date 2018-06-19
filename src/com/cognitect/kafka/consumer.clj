(ns com.cognitect.kafka.consumer
  (:require [clojure.spec.alpha            :as s]
            [clojure.walk                  :as walk]
            [clojure.stacktrace            :as stacktrace]
            [io.pedestal.log               :as log]
            [io.pedestal.interceptor.chain :as interceptor.chain]
            [com.cognitect.kafka.common    :as common]
            [com.cognitect.kafka.topic     :as topic])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerInterceptor ConsumerRecord ConsumerRecords MockConsumer OffsetAndMetadata OffsetResetStrategy]
           [java.util.concurrent Executors]
           [org.apache.kafka.common.serialization ByteArrayDeserializer Deserializer StringDeserializer]
           [org.apache.kafka.common.errors WakeupException]))

(s/def ::key.deserializer               (common/names-kindof? Deserializer))
(s/def ::value.deserializer             (common/names-kindof? Deserializer))

(s/def ::auto.commit.interval.ms        ::common/time)
(s/def ::auto.offset.reset              string?)
(s/def ::check.crcs                     common/bool-string?)
(s/def ::enable.auto.commit             common/bool-string?)
(s/def ::exclude.internal.topics        common/bool-string?)
(s/def ::fetch.max.wait.ms              ::common/time)
(s/def ::fetch.min.bytes                ::common/size)
(s/def ::group.id                       string?)
(s/def ::heartbeat.interval.ms          ::common/time)
(s/def ::interceptor.classes            (common/names-kindof? ConsumerInterceptor))
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
                                     ::common/sasl.kerberos.ticker.renew.window.factor]))

(def string-deserializer     (.getName StringDeserializer))
(def byte-array-deserializer (.getName ByteArrayDeserializer))

(defn create-consumer
  [config]
  {:pre [(s/valid? ::configuration config)]}
  (KafkaConsumer. (common/config->properties config)))

(defn create-mock
  []
  (MockConsumer. OffsetResetStrategy/EARLIEST))

(defn- consumer-record->map
  [^ConsumerRecord record]
  {:checksum              (.checksum record)
   :key                   (.key record)
   :offset                (.offset record)
   :partition             (.partition record)
   :serialized-key-size   (.serializedKeySize record)
   :serialized-value-size (.serializedValueSize record)
   :timestamp             (.timestamp record)
   :timestamp-type        (.timestampType record)
   :topic                 (.topic record)
   :value                 (.value record)
   :consumer-record       record})

(defn- dispatch-record
  [consumer interceptors ^ConsumerRecord record]
  (let [context {:consumer consumer
                 :message  (consumer-record->map record)}]
    (log/debug :in :poll-and-dispatch :context context)
    (log/counter :io.pedestal/active-kafka-messages 1)
    (try
      (let [final-context (interceptor.chain/execute context interceptors)]
        (log/debug :msg "leaving interceptor chain" :final-context final-context))
      (catch Throwable t
        (log/meter ::dispatch-error)
        (log/error :msg "Dispatch code threw an exception"
                   :throwable t
                   :cause-trace (with-out-str (stacktrace/print-cause-trace t))))
      (finally
        (log/counter :io.pedestal/active-kafka-messages -1)))))

(defn- poll-and-dispatch
  [interceptors consumer]
  (let [^ConsumerRecords msgs (.poll consumer (long 100))]
    (when (< 0 (.count msgs))
      (doseq [record (iterator-seq (.iterator msgs))]
        (dispatch-record consumer interceptors record)))))

(defn- start-loop
  [consumer interceptors topic-names auto-close?]
  (let [continue?  (atom true)
        _          (.subscribe consumer topic-names)
        completion (future
                     (try
                       (while @continue?
                         (try
                           (poll-and-dispatch interceptors consumer)
                           (catch WakeupException _)))
                       :ok
                       (catch Throwable t t)
                       (finally
                         (log/info :msg "Exiting receive loop")
                         (when auto-close?
                           (.close consumer)))))]
    {:kafka-consumer consumer
     :continue?      continue?
     :completion     completion}))

(def error-logger
  {:name  ::error-logger
   :error (fn [context exception]
            (log/error :msg       "Error reached front of chain"
                       :exception exception
                       :context   context)
            context)})

(def default-interceptors
  [error-logger])

(defn start-consumer
  [consumer auto-close? service-map]
  (let [topic-names  (map ::topic/name (::topic/topics service-map))
        config       (::configuration service-map)
        interceptors (::interceptors service-map)
        interceptors (into default-interceptors interceptors)]
    (start-loop consumer interceptors topic-names auto-close?)))

(defn stop-consumer
  [loop-state]
  (reset! (:continue? loop-state) false)
  (.wakeup (:kafka-consumer loop-state))
  (deref (:completion loop-state) 100 :timeout))

;; ----------------------------------------
;; Utility functions

(defn commit-sync [{consumer :consumer}]
  (when consumer
    (log/debug :msg "commit sync")
    (.commitSync ^KafkaConsumer consumer)))

(defn commit-message-offset [{consumer :consumer message :message}]
  (when (and consumer message)
    (let [commit-point (long (inc (.offset ^ConsumerRecord message)))]
      (log/debug :msg (str "commit at " commit-point))
      (.commit consumer (java.util.Collections/singletonMap
                         (:partition message)
                         (OffsetAndMetadata. commit-point))))))
