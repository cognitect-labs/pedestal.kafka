(ns io.pedestal.kafka
  (:require [clojure.spec :as s]
            [io.pedestal.kafka.connector :as connector]
            [io.pedestal.kafka.producer :as producer]
            [io.pedestal.kafka.consumer :as consumer]))

(s/def ::start-fn        fn?)
(s/def ::stop-fn         fn?)

(s/def ::topic-name      string?)
(s/def ::topic-settings  (s/keys))
(s/def ::topics          (s/map-of ::topic-name ::topic-settings))

(s/def ::service-map-in  (s/keys :req [::topics] :opt [::consumer/configuration ::producer/configuration]))
(s/def ::service-map-out (s/keys :req-un [::start-fn ::stop-fn]))

(defn kafka-server
  "Transforms a service map by adding :start-fn, :stop-fn,
  and :server. The resulting server will consume Kafka messages and
  dispatch them to an interceptor stack.

  The topics to consume are defined by the ::topics key."
  [service-map]
  (let [args (s/conform ::service-map-in service-map)]
    (if (= args ::s/invalid)
      (throw (ex-info "Service map is not valid" (s/explain-data ::service-map-in service-map)))
      (assoc service-map
             :start-fn (fn [])
             :stop-fn  (fn []))))
  )

(s/fdef kafka-server
        :args (s/cat :service-map ::service-map-in)
        :ret  ::service-map-out)


(comment

  (kafka-server {::topics {}
                 ::consumer/configuration {}
})


  )
