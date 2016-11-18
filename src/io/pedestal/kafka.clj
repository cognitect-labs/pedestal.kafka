(ns io.pedestal.kafka
  (:require [clojure.spec :as s]
            [io.pedestal.kafka.common    :as common]
            [io.pedestal.kafka.connector :as connector]
            [io.pedestal.kafka.producer  :as producer]
            [io.pedestal.kafka.consumer  :as consumer]
            [io.pedestal.kafka.topic     :as topic]))

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
  (-> service-map
      (update ::consumers consumer/stop-consumer service-map)
      (assoc  ::start-fn starter)
      (dissoc ::stop-fn)))

(def stop  (service-fn ::stop-fn))

(s/fdef stop
        :args (s/cat :service-map ::service-map-in)
        :ret  ::service-map-stopped)

(defn- starter
  [service-map]
  {:pre  [(s/valid? ::service-map-in service-map)]
   :post [(s/valid? ::service-map-started %)]}
  (-> service-map
      (update ::consumers consumer/start-consumer service-map)
      (assoc  ::stop-fn stopper)
      (dissoc ::start-fn)))

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

  (def service-map {::topic/topics           [{::topic/name "smoketest" ::topic/parallelism 1}]
                    ::consumer/configuration {::common/bootstrap.servers    "localhost:9902"
                                              ::consumer/key.deserializer   consumer/string-deserializer
                                              ::consumer/value.deserializer consumer/string-deserializer}
                    })



  )
