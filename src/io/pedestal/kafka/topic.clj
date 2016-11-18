(ns io.pedestal.kafka.topic
  (:require [clojure.spec :as s]
            [io.pedestal.kafka.common :as common]))

(s/def ::name        string?)
(s/def ::settings    (s/keys :req [::name]))
(s/def ::topics      (s/coll-of ::settings))
