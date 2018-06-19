(ns com.cognitect.kafka.topic
  (:require [clojure.spec.alpha         :as s]
            [com.cognitect.kafka.common :as common]))

(s/def ::name        string?)
(s/def ::settings    (s/keys :req [::name]))
(s/def ::topics      (s/coll-of ::settings))
