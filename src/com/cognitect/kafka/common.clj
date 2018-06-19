(ns com.cognitect.kafka.common
  (:require [clojure.spec.alpha         :as s]
            [com.cognitect.kafka.common :as common]
            [clojure.walk               :as walk])
  (:import [java.util Properties]))

(defn names-implementer? [classname interface]
  (.isAssignableFrom interface (Class/forName classname)))

(defn names-kindof? [interface] (s/and string? #(names-implementer? % interface)))

(def bool-string?    (s/and string? #{"true" "false"}))
(def int-string?     (s/and string? #(try (Integer/parseInt %) (catch NumberFormatException _ false))))
(def pos-int-string? (s/and string? #(try (pos? (Integer/parseInt %)) (catch NumberFormatException _ false))))

(s/def ::size                                     pos-int-string?)
(s/def ::time                                     pos-int-string?)

(s/def ::bootstrap.servers                        string?)

(s/def ::client.id                                string?)
(s/def ::connections.max.idle.ms                  ::time)
(s/def ::metadata.max.age.ms                      ::time)
(s/def ::metric.reporters                         (s/coll-of string?))
(s/def ::metrics.num.samples                      ::size)
(s/def ::metrics.sample.window.ms                 ::time)
(s/def ::receive.buffer.bytes                     ::size)
(s/def ::reconnect.backoff.ms                     ::time)
(s/def ::request.timeout.ms                       ::time)
(s/def ::retry.backoff.ms                         ::time)
(s/def ::security.protocol                        string?)
(s/def ::send.buffer.bytes                        ::size)

(s/def ::ssl.key.password                         string?)
(s/def ::ssl.keystore.location                    string?)
(s/def ::ssl.keystore.password                    string?)
(s/def ::ssl.truststore.location                  string?)
(s/def ::ssl.truststore.password                  string?)
(s/def ::ssl.enabled.protocols                    (s/coll-of string? :kind list?))
(s/def ::ssl.keystore.type                        string?)
(s/def ::ssl.protocol                             string?)
(s/def ::ssl.provider                             string?)
(s/def ::ssl.truststore.type                      string?)
(s/def ::ssl.cipher.suites                        (s/coll-of string? :kind list?))
(s/def ::ssl.endpoint.identification.algorithm    string?)
(s/def ::ssl.keymanager.algorithm                 string?)
(s/def ::ssl.trustmanager.algorithm               string?)

(s/def ::sasl.kerberos.service.name               string?)
(s/def ::sasl.mechanism                           string?)
(s/def ::sasl.kerberos.kinit.cmd                  string?)
(s/def ::sasl.kerberos.min.time.before.relogin    ::time)
(s/def ::sasl.kerberos.ticket.renew.jitter        double?)
(s/def ::sasl.kerberos.ticker.renew.window.factor double?)

(defn ^Properties map->properties
  "Translate a Clojure map into a java.util.Properties object.
   All keys in the map must be strings."
  [m]
  (let [p (Properties.)]
    (doseq [[k v] m]
      (.setProperty p k v))
    p))

(def config->properties (comp common/map->properties walk/stringify-keys))
