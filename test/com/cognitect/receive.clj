(ns com.cognitect.receive
  (:require [clojure.test :refer :all]
            [com.cognitect.kafka          :as k]
            [com.cognitect.kafka.common   :as common]
            [com.cognitect.kafka.consumer :as consumer]
            [com.cognitect.kafka.topic    :as topic]
            [clojure.spec :as s]))

(def minimal-configuration
  {::topic/topics           [{::topic/name "test-message-receipt"}]
   ::consumer/configuration {::common/bootstrap.servers    "localhost:9092"
                             ::common/group.id             "unit-tests"
                             ::consumer/key.deserializer   consumer/string-deserializer
                             ::consumer/value.deserializer consumer/string-deserializer}})

(defn- consumer-with
  [topic-partitions messages]
  (assert (= (distinct (keys topic-partitions)) (distinct (map :topic messages))))
  (consumer/create-mock topic-partitions messages))

(defmacro m
  [topic offset key value]
  `{:topic  ~topic
    :offset ~offset
    :key    ~key
    :value  ~value})

(defmacro ms
  [& ms]
  (into [] (map #(list* `m %) (partition 4 ms))))

(defn- use-mock
  [service-map topic-partitions messages]
  (assoc service-map ::k/consumer
         (consumer-with topic-partitions messages)))

(defn- server
  [consumer]
  (-> minimal-configuration
      (assoc ::k/consumer consumer)
      k/start))

(defn recorder
  [accum]
  {:name ::recorder
   ::enter (fn [context]
             (swap! accum conj (:message context))
             context)})

(deftest one-partition
  (let [transcript    (atom [])
        srv           (-> minimal-configuration
                          (use-mock {"receive-0" 0} (ms "receive-0" 0 "k1" "v1"))
                          (assoc ::consumer/interceptors [(recorder transcript)])
                          k/kafka-server
                          k/start)]
    )
  )

(run-tests)
