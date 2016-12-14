(ns com.cognitect.lifecycle
  (:require [clojure.test :refer :all]
            [com.cognitect.kafka          :as k]
            [com.cognitect.kafka.common   :as common]
            [com.cognitect.kafka.consumer :as consumer]
            [com.cognitect.kafka.topic    :as topic]
            [clojure.spec :as s]))

(def minimal-configuration
  {::topic/topics           [{::topic/name "smoketest"}]
   ::consumer/configuration {::common/bootstrap.servers    "localhost:9092"
                             ::common/group.id             "unit-tests"
                             ::consumer/key.deserializer   consumer/string-deserializer
                             ::consumer/value.deserializer consumer/string-deserializer}})

(defn attempt [m]
  (-> m
      k/kafka-server
      k/start))

(deftest minimal-configuration-passes-spec
  (is (s/valid? ::k/service-map-in minimal-configuration)))

(deftest attempt-mock-consumer
  (-> minimal-configuration
      (assoc ::consumer/configuration :mock)
      k/kafka-server
      k/start))

(deftest topic-list-is-required
  (is (thrown? AssertionError (attempt (dissoc minimal-configuration ::topic/topics)))))

(deftest consumer-is-required
  (is (thrown? AssertionError (attempt (dissoc minimal-configuration ::consumer/configuration)))))

(deftest start-stop-cycle
  (is (not (nil? (-> minimal-configuration attempt))))
  (is (not (nil? (-> minimal-configuration attempt k/stop))))
  (is (not (nil? (-> minimal-configuration attempt k/stop k/start))))
  (is (not (nil? (-> minimal-configuration attempt k/stop k/start k/stop)))))
