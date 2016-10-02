(defproject pedestal.kafka "0.1.0-SNAPSHOT"
  :description  "Chain provider and interceptors for using Pedestal with Kafka"
  :url          "http://gitlab.com/mtnygard/pedestal.kafka"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.9.0-alpha13"]
                 [org.clojure/core.async           "0.2.391"]
                 [io.pedestal/pedestal.interceptor "0.5.1"]
                 [io.pedestal/pedestal.route       "0.5.1"]
                 [org.apache.kafka/kafka_2.11      "0.10.0.1"]
                 [org.apache.zookeeper/zookeeper   "3.4.9"]])
