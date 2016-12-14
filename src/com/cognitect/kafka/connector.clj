(ns com.cognitect.kafka.connector
  (:import kafka.consumer.RangeAssignor
           kafka.message.MessageAndMetadata))

(def range-assignor  (.getName RangeAssignor))

;; ----------------------------------------
;; Transducers for interpreting messages
(def
  ^{:doc "Expects {::body Any}
          Emits   Any"}
  extract-body
  (map (fn [m]
         (::body m))))

(def
  ^{:doc "Expects {::body      org.apache.kafka.clients.consumer.ConsumerRecord}
          Emits   {::topic     Any
                   ::body      Any
                   ::partition integer
                   ::key       Any
                   ::offset    long}"}
  extract-message
  (map (fn [^MessageAndMetadata m]
         {::key       (.key m)
          ::offset    (.offset m)
          ::partition (.partition m)
          ::topic     (.topic m)
          ::body      (.message m)})))

(def decode-string-message-utf8
  (map (fn [m] (update m ::body #(String. ^bytes % java.nio.charset.StandardCharsets/UTF_8)))))

;; ----------------------------------------
;; Transducers for sending messages
(def ^{:doc "Expects {::body UTF-8 String
             Emits   {::body byte-array"}
  encode-string-message-utf8
  (map (fn [m] (update m ::body #(.getBytes ^String % java.nio.charset.StandardCharsets/UTF_8)))))

(def
  ^{:doc "Expects Any
          Emits   {::body Any}"}
  attach-body
  (map (fn [v] {::body v})))
