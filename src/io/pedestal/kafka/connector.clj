(ns io.pedestal.kafka.connector
  "Channel connector for Kafka topics.

   `topic->channel!` makes messages from a topic appear as messages on a channel."
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.walk :as walk]
            [clojure.edn :as edn])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream StringBufferInputStream]
           [java.util Properties]
           [java.util.concurrent Executors]
           [kafka.consumer Consumer ConsumerConfig KafkaStream RangeAssignor]
           [org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer StringSerializer StringDeserializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [kafka.javaapi.consumer ConsumerConnector]
           [kafka.message MessageAndMetadata]))

(def string-serializer       (.getName StringSerializer))
(def string-deserializer     (.getName StringDeserializer))
(def byte-array-serializer   (.getName ByteArraySerializer))
(def byte-array-deserializer (.getName ByteArrayDeserializer))
(def range-assignor          (.getName RangeAssignor))

;; ----------------------------------------
;; Consumer configuration and connection
(defn ^Properties map->properties
  "Translate a Clojure map into a java.util.Properties object.
   All keys in the map must be strings."
  [m]
  (let [p (Properties.)]
    (doseq [[k v] m]
      (.setProperty p k v))
    p))

(defn consumer-connector
  "Build a Kafka ConsumerConnector from the supplied configuration map.

  The configuration map can have the same keys as
  kafka.consumer.ConsumerConfig, written as keywords. For example the
  keyword :zookeeper.connect equates to the property
  \"zookeeper.connect\"

   :zookeeper.connect and :group.id are the minimum needed to have a
  valid configuration.

   No defaults are provided for other keys. The application must
   supply its own appropriate configuration values."
  [config]
  (-> (walk/stringify-keys config)
      (map->properties)
      (ConsumerConfig.)
      (Consumer/createJavaConsumerConnector)))

;; ----------------------------------------
;; Mapping a topic onto a channel
(defn message-streams
  "Create a collection of message streams on the given
  topic. Parallelism will determine how many streams to create.

  Returns a list of the streams that were created."
  [^ConsumerConnector connector topic parallelism]
  (let [stream-map (.createMessageStreams connector {topic parallelism})]
    (get stream-map topic)))

(defn- receive-loop
  "Returns a function that will start a consumer loop over the given
  stream. Each received message will be put onto the supplied
  channel.

  Once started, the loop will continue running until the stream is
  shutdown."
  [message-count stream channel]
  (fn []
    (loop [iter (.iterator stream)]
      (when (.hasNext iter)
        (let [msg (.next iter)]
          (async/>!! channel msg)
          (send message-count inc)
          (recur iter))))))

(defn start-threads!
  "Starts one thread per stream, initiating a receiver loop on each.

   Returns a java.util.concurrent.ExecutorService that can later be
   used to shut down the threads."
  [message-count streams channel]
  (let [pool (Executors/newFixedThreadPool (count streams))]
    (doseq [stream streams]
      (.submit pool (receive-loop message-count stream channel)))
    pool))

(defn shutdown-fn
  "Create a thunk to do a clean shutdown of streams and the Kafka
  ConsumerConnector."
  [connector streams channel]
  (fn []
    (.shutdown connector)
    (.shutdown streams)))

(defn topic->channel!
  "Begins mapping a Kafka topic onto the given channel. It will
  consume messages with the specified parallelism, starting one thread
  per stream.

  Configuration values are as specified by `consumer-connector`.

  The topic name must be a String.

  The calling application supplies a channel with the buffer and
  semantics that it requires. If the channel has a fixed size buffer
  and that buffer fills up, then the receiver loops will block.

  Returns a map with a key :shutdown. The value of that key is
  a thunk that can be used for clean shutdown later.

  The returned map also contains a key :metrics whose value is an agent
  that contains the number of messages received."
  [config topic parallelism channel]
  (let [message-count (agent 0)
        connector     (consumer-connector config)
        streams       (message-streams connector topic (int parallelism))
        threads       (start-threads! message-count streams channel)]
    {:shutdown (shutdown-fn connector threads streams)
     :metrics  message-count}))

;; ----------------------------------------
;; Mapping a channel onto a topic
(defn producer
  "Return a producer that can be used for one or more topic->channel!
  mappings. The configs are documented at
  http://kafka.apache.org/documentation.html#producerconfigs

  The configuration map can have the same keys as
  org.apache.kafka.clients.producer.ProducerConfig, written as
  keywords for easier use from Clojure. For example the
  keyword :bootstrap.servers equates to the property
  \"bootstrap.servers\""
  [config]
  (-> config
      (walk/stringify-keys)
      (KafkaProducer.)))

(defn channel->topic!
  "Begins mapping the given channel to a Kafka topic for the
  producer. This runs as an asynchronous go loop.

  The topic name must be a String.

  The values coming from the channel must implement
  kafka.message.Message. This namespace has some useful transducers
  for creating those messages from ordinary Clojure maps.

  The calling application supplies a channel with the buffer and
  semantics that it requires.

  To stop this mapping, close the channel.

  Message sends are asynchronous. If you need to confirm when the send
  completes, you can supply a futures-chan. The futures-chan will
  get [message future] pairs.

  Returns a map with the key :metrics. It's value is an agent that
  counts the number of messages sent."
  ([producer topic channel]
   (channel->topic! producer topic channel (async/chan (async/dropping-buffer 1))))
  ([producer topic channel futures-chan]
   (let [message-count (agent 0)]
     (async/go-loop []
       (when-let [msg (async/<! channel)]
         (let [rec     (ProducerRecord. topic (::body msg))
               confirm (.send producer rec)]
           (async/>! futures-chan [msg confirm]))
         (send message-count inc)
         (recur)))
     {:metrics message-count})))

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

;; ----------------------------------------
;; Support for literal configurations
(defn- buf-or-n [arg]
  (cond
    (integer? arg)                   arg
    (not (sequential? arg))          arg
    (= :sliding-buffer (first arg))  (async/sliding-buffer (second arg))
    (= :dropping-buffer (first arg)) (async/dropping-buffer (second arg))))

(defn- subscribe-channel
  [{:keys [topic consumer parallelism buffer] :or {buffer 1 parallelism 1}}]
  (let [receiver (async/chan (buf-or-n buffer))
        consumer (topic->channel! consumer topic parallelism receiver)]
    {:channel receiver :consumer consumer}))

(defn- publish-channel
  [{:keys [topic buffer] :or {buffer 1} :as config}]
  (let [sender   (async/chan (buf-or-n buffer))
        producer (producer (:producer config))]
    {:channel sender :producer (channel->topic! producer topic sender)}))

(defn read-subscription
  [form]
  (fn []
    (subscribe-channel form)))

(defn read-publication
  [form]
  (fn []
    (publish-channel form)))


(comment

  (def recv (async/chan (async/sliding-buffer 1000) (comp extract-message decode-string-message-utf8)))

  (def kconfig {:zookeeper.connect "localhost:2181"
                :group.id          "prod"})

  (def shut-it-down (topic->channel! kconfig "content" 10 recv))

  (def last-in (atom nil))
  (async/take! recv (fn [m] (reset! last-in m)))

  @last-in

  ((:shutdown shut-it-down))

  (def prod (async/chan (async/dropping-buffer 5) attach-body))
  (def p (producer {:bootstrap.servers "localhost:9092"
                    :key.serializer    ByteArraySerializer
                    :value.serializer  ByteArraySerializer}))

  (channel->topic! p "content" prod)

  (async/put! prod {:type :message.type/lookup :name "cat0001"})


  )
