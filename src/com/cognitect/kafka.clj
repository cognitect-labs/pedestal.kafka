(ns com.cognitect.kafka
  (:require [clojure.spec.alpha            :as s]
            [com.cognitect.kafka.common    :as common]
            [com.cognitect.kafka.connector :as connector]
            [com.cognitect.kafka.producer  :as producer]
            [com.cognitect.kafka.consumer  :as consumer]
            [com.cognitect.kafka.topic     :as topic]
            [com.cognitect.kafka.parser    :as parser]
            [io.pedestal.log               :as log]))

(s/def ::start-fn             fn?)
(s/def ::stop-fn              fn?)
(s/def ::service-map-in       (s/keys :req [::topic/topics]
                                      :opt [::consumer/configuration ::producer/configuration ::start-fn ::stop-fn ::consumer]))
(s/def ::service-map-stopped  (s/keys :req [::start-fn]))
(s/def ::service-map-started  (s/keys :req [::stop-fn]
                                      :opt [::consumer-loop]))

(defmacro service-fn [k]
  `(fn [service-map#]
     (if-let [f# (get service-map# ~k)]
       (f# service-map#)
       service-map#)))

(declare starter)

(defn- stopper
  [service-map]
  {:pre  [(s/valid? ::service-map-in service-map)]
   :post [(s/valid? ::service-map-stopped %)]}
  (let [shutdown-result (consumer/stop-consumer (::consumer-loop service-map))]
    (-> service-map
        (assoc ::consumer-shutdown shutdown-result)
        (dissoc ::consumer-loop ::stop-fn)
        (assoc ::start-fn (fn [& _] (assert false "This service cannot be restarted."))))))

(def stop  (service-fn ::stop-fn))

(s/fdef stop
        :args (s/cat :service-map ::service-map-in)
        :ret  ::service-map-stopped)

(defn- starter
  [service-map]
  {:pre  [(s/valid? ::service-map-in service-map)]
   :post [(s/valid? ::service-map-started %)]}
  (let [created? (not (some? (::consumer service-map)))
        consumer (or (::consumer service-map) (consumer/create-consumer (::consumer/configuration service-map)))
        loop     (consumer/start-consumer consumer created? service-map)]
    (-> service-map
        (assoc ::consumer          consumer
               ::consumer-created? created?
               ::consumer-loop     loop
               ::stop-fn           stopper)
        (dissoc ::start-fn))))

(def start (service-fn ::start-fn))

(s/fdef start
        :args (s/cat :service-map ::service-map-in)
        :ret  ::service-map-started)

(defn configuration-problems
  [service-map]
  (when-not (s/valid? ::service-map-in service-map)
    (s/explain-data ::service-map-in service-map)))

(defn kafka-server
  [service-map]
  (assoc service-map ::start-fn starter))

(comment

  (def printerceptor
    {:name ::printerceptor
     :enter (fn [context]
              (println (:message context))
              context)})

  (def service-map {::topic/topics           [{::topic/name "smoketest" ::topic/parallelism 1}]
                    ::consumer/configuration {::common/bootstrap.servers          "localhost:9092"
                                              ::common/group.id                   "development"
                                              ::consumer/key.deserializer         consumer/string-deserializer
                                              ::consumer/value.deserializer       consumer/string-deserializer
                                              ::consumer/enable.auto.commit       "true"
                                              ::consumer/auto.commit.interval.ms  "1000"
                                              ::consumer/session.timeout.ms       "30000"}
                    ::consumer/interceptors  [printerceptor]
                    })

  (def s  (-> service-map kafka-server start))

  (stop s)

  )


;; ----------------------------------------
;; Interceptors

(def commit-sync
  {:name ::commit-sync
   :enter
   (fn [context]
     (consumer/commit-sync context)
     context)})

(def commit-message
  {:name ::commit-per-message
   :enter
   (fn [context]
     (consumer/commit-message-offset context)
     context)})

(def edn-value parser/edn-value)
(def json-value parser/json-value)
(def transit-json-value parser/transit-json-value)
(def transit-msgpack-value parser/transit-msgpack-value)

(comment

  (def service-map {::topic/topics           [{::topic/name "smoketest" ::topic/parallelism 1}]
                    ::consumer/configuration {::common/bootstrap.servers          "localhost:9092"
                                              ::common/group.id                   "development-2"
                                              ::consumer/key.deserializer         consumer/string-deserializer
                                              ::consumer/value.deserializer       consumer/string-deserializer
                                              ::consumer/enable.auto.commit       "false"
                                              ::consumer/session.timeout.ms       "30000"}
                    ::consumer/interceptors  [(edn-value) printerceptor commit-message]
                    })

  (def s  (-> service-map kafka-server start))

  (stop s)

  )
