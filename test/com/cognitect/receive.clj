(ns com.cognitect.receive
  (:require [clojure.test :refer :all]
            [com.cognitect.kafka          :as k]
            [com.cognitect.kafka.common   :as common]
            [com.cognitect.kafka.consumer :as consumer]
            [com.cognitect.kafka.topic    :as topic]
            [clojure.spec :as s])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition]))

(def minimal-configuration
  {::topic/topics           [{::topic/name "test-topic"}]
   ::consumer/configuration {::common/bootstrap.servers    "localhost:9092"
                             ::common/group.id             "unit-tests"
                             ::consumer/key.deserializer   consumer/string-deserializer
                             ::consumer/value.deserializer consumer/string-deserializer}})

(defn- ->topic-partition
  [[topic partition & _]]
  (TopicPartition. topic partition))

(defn- ->record
  [[topic partition offset key value]]
  (ConsumerRecord. topic partition offset key value))

(defn- mock-consumer
  [messages]
  (let [topics  (distinct (map first messages))
        tps     (distinct (map ->topic-partition messages))
        records (map ->record messages)
        offsets (zipmap tps (repeat 0))
        consumer (consumer/create-mock)]
    (doto consumer
      (.subscribe topics)
      (.rebalance tps)
      (.updateBeginningOffsets offsets))
    (doseq [r records]
      (.addRecord consumer r))
    consumer))

(defn when-msg
  [pr f]
  {:name ::when-msg
   :enter (fn [context]
            (when (pr (:message context))
              (f))
            context)})

(defn- run-for
  [duration interceptors messages]
  (let [latch        (promise)
        end-signal   (nth (last messages) 3)
        stop-sign    (when-msg #(= end-signal (:key %))
                       #(deliver latch :all-messages-delivered))
        interceptors (conj interceptors stop-sign)
        consumer     (mock-consumer messages)
        srv          (-> minimal-configuration
                         k/kafka-server
                         (assoc ::consumer/interceptors interceptors)
                         (assoc ::k/consumer consumer)
                         k/start)
        exit-val     (deref latch duration :timeout)]
    (is (= :all-messages-delivered exit-val))
    (let [srv (k/stop srv)]
      (is (= :ok (::k/consumer-shutdown srv)))
      srv)))

(defn recorder
  []
  (let [record (atom [])]
    {:name   ::recorder
     :record record
     :enter  (fn [context]
               (swap! record conj (:message context))
               context)}))

(defn ms
  [& msgs]
  (into [] (map vec (partition 5 msgs))))

(deftest one-partition
  (let [transcript (recorder)
        messages   (ms "test-topic" 0 0 "k1" "v1"
                       "test-topic" 0 1 "k2" "v2"
                       "test-topic" 0 2 "end" "end")]
    (run-for 200 [transcript] messages)
    (is (= (count messages) (count @(:record transcript))))))
