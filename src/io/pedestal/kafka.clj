(ns io.pedestal.kafka
  (:require [clojure.spec :as s]
            [io.pedestal.http.body-params :as body-params]
            [io.pedestal.kafka.common    :as common]
            [io.pedestal.kafka.connector :as connector]
            [io.pedestal.kafka.producer  :as producer]
            [io.pedestal.kafka.consumer  :as consumer]
            [io.pedestal.kafka.topic     :as topic]
            [io.pedestal.kafka.parser    :as parser]
            [io.pedestal.log :as log]))

(s/def ::start-fn             fn?)
(s/def ::stop-fn              fn?)
(s/def ::service-map-in       (s/keys :req [::topic/topics]
                                      :opt [::consumer/configuration ::producer/configuration ::start-fn ::stop-fn]))
(s/def ::service-map-stopped  (s/keys :req [::start-fn]))
(s/def ::service-map-started  (s/keys :req [::stop-fn]))

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
  (let [shutdown-result (consumer/stop-consumer (::consumer service-map))]
    (-> service-map
        (assoc ::consumer-shutdown shutdown-result)
        (dissoc ::consumer ::stop-fn)
        (assoc ::start-fn starter))))

(def stop  (service-fn ::stop-fn))

(s/fdef stop
        :args (s/cat :service-map ::service-map-in)
        :ret  ::service-map-stopped)

(defn- starter
  [service-map]
  {:pre  [(s/valid? ::service-map-in service-map)]
   :post [(s/valid? ::service-map-started %)]}
  (let [consumer (consumer/start-consumer service-map)]
    (-> service-map
        (assoc ::consumer consumer
               ::stop-fn stopper)
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
