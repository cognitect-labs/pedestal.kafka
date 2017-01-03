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
