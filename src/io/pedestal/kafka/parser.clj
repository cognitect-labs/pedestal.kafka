(ns io.pedestal.kafka.parser
  (:require [clojure.edn :as edn]
            [cheshire.core :as json]
            [cheshire.parse :as parse]
            [clojure.string :as str]
            [io.pedestal.log :as log]
            [cognitect.transit :as transit])
  (:import [java.io StringReader PushbackReader]))

(defn edn-parser
  "Return a parser fn that will read a string value with
  `edn/read`. The `options` map is passed along to `edn/read`."
  [options value]
  (let [edn-options (assoc options :eof nil)]
    (-> value
        StringReader.
        PushbackReader.
        (->> (edn/read edn-options)))))

(defn json-parser
  "Return a json-parser fn that reads a value with
  `json/read`. options are key-val pairs as follows:

    :bigdec Boolean value which defines if numbers are parsed as BigDecimal
            or as Number with simplest/smallest possible numeric value.
            Defaults to false.

    :key-fn Key coercion, where true coerces keys to keywords, false leaves
            them as strings, or a function to provide custom coercion.

    :array-coerce-fn Define a collection to be used for array values by name."
  [options value]
    (let [{:keys [bigdec key-fn array-coerce-fn]
         :or {bigdec          false
              key-fn          keyword
              array-coerce-fn nil}} options]
    (binding [parse/*use-bigdecimals?* bigdec]
      (json/parse-stream
       (-> value StringReader. PushbackReader.)
       key-fn array-coerce-fn))))

(defn transit-parser
  "Return a parser fn that will read a value `transit/read`. The
  `options` map is passed along to transit/reader"
  [format options value]
  (transit/read (transit/reader value format options)))

(defn parse-message-with
  [parser-fn]
  {:name ::value-parser
   :enter
   (fn [context]
     (assoc-in context [:message :parsed-value]
               (parser-fn (get-in context [:message :value]))))})

(defn edn-value             [& {:as options}] (parse-message-with (partial edn-parser options)))
(defn json-value            [& {:as options}] (parse-message-with (partial json-parser options)))
(defn transit-json-value    [& {:as options}] (parse-message-with (partial transit-parser :json options)))
(defn transit-msgpack-value [& {:as options}] (parse-message-with (partial transit-parser :msgpack options)))
