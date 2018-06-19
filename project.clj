(defproject com.cognitect/pedestal.kafka "0.2.0-SNAPSHOT"
  :description  "Chain provider and interceptors for using Pedestal with Kafka"
  :url          "http://gitlab.com/mtnygard/pedestal.kafka"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.9.0"]
                 [org.clojure/core.async           "0.4.474"]
                 [io.pedestal/pedestal.interceptor "0.5.3"]
                 [io.pedestal/pedestal.route       "0.5.3"]
                 [io.pedestal/pedestal.service     "0.5.3"]
                 [cheshire                         "5.8.0"]
                 [com.cognitect/transit-clj        "0.8.309"]
                 [commons-codec                    "1.11"]
                 [org.apache.kafka/kafka_2.11      "1.1.0"]
                 [org.apache.zookeeper/zookeeper   "3.4.12"]])
