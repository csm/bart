(ns bart.struct
  (:require [clojure.spec.alpha :as s]
            [clojure.walk :refer [keywordize-keys]])
  (:import [io.netty.buffer ByteBuf ByteBufUtil ByteBufInputStream ByteBufAllocator ByteBufOutputStream]
           [org.bson BasicBSONDecoder BasicBSONObject BasicBSONEncoder]
           [java.io EOFException]))

(s/def :field/name keyword?)
(s/def :field/type #{:int32 :int64 :cstring :document :section})
(s/def :field/repeated boolean?)
(s/def :field/optional boolean?)
(s/def :field/skip boolean?)

(s/def ::field-def
  (s/and (s/keys :req-un [:field/name :field/type]
                 :opt-un [:field/repeated :field/optional :field/skip])
         #(or (not (:skip %))
              (#{:int32 :int64} (:type %)))))

(s/def ::struct-def
  (s/and (s/coll-of ::field-def)
         #(= (count (map :name %)) (map :name %))))

(defn enforce
  [pred value]
  (if (pred value)
    value
    (throw (IllegalArgumentException. "enforcement failed"))))

(defn read-bson
  [buffer]
  (-> ^BasicBSONObject (.readObject (BasicBSONDecoder.) (ByteBufInputStream. buffer))
    (.toMap)
    (->> (into {}))
    (keywordize-keys)))

(defn maybe-read-bson
  [buffer]
  (try
    (read-bson buffer)
    (catch EOFException _ nil)))

(defn read-cstring
  [buffer]
  (let [len (.bytesBefore buffer 0)]
    (when (<= 0 len)
      (let [buf (byte-array len)]
        (.readBytes buffer buf)
        (.readByte buffer)
        (String. buf "UTF-8")))))

(defn struct-reader
  "Produce a function that parses a io.netty.buffer.ByteBuf into a map
  of values."
  [defs]
  (s/conform ::struct-def defs)
  (fn [^ByteBuf buffer]
    (let [result (volatile! (transient {}))]
      (doseq [{:keys [name type repeated optional skip] :or {repeated false optional false skip false}} defs]
        (let [read-value (fn []
                           (case type
                             :int32 (when (<= 4 (.readableBytes buffer))
                                      (.readIntLE buffer))
                             :int64 (when (<= 8 (.readableBytes buffer))
                                      (.readLongLE buffer))
                             :cstring (read-cstring buffer)
                             :document (try
                                         (read-bson buffer)
                                         (catch EOFException e
                                           nil))
                             :section (try
                                        (let [code (.getByte buffer (.readerIndex buffer))]
                                          (case code
                                            0 (do
                                                (.readByte buffer)
                                                {:body (read-bson buffer)})
                                            1 (do
                                                (let [len (.getIntLE buffer (inc (.readerIndex buffer)))])
                                                (let [len (.readIntLE buffer)
                                                      seq-id (read-cstring buffer)]
                                                  {:len len :seq-id seq-id :documents (take-while some? (repeatedly #(maybe-read-bson buffer)))}))
                                            nil))
                                        (catch IndexOutOfBoundsException _ nil)
                                        (catch EOFException _ nil))))

              value (cond
                      repeated (take-while some? (repeatedly read-value))
                      optional (read-value)
                      :else (enforce some? (read-value)))]
          (when-not skip
            (vswap! result assoc! name value))))
      (persistent! @result))))

(defn write-bson
  [buffer value]
  (let [doc (BasicBSONObject. (into {} (map (fn [[k v]] [(name k) v]) value)))]
    (.putObject (doto (BasicBSONEncoder.) (.set (ByteBufOutputStream. buffer))) doc)))

(defn struct-writer
  "Produce a function than encodes a value according to defs into a io.netty.buffer.ByteBuf."
  ([defs] (struct-writer defs ByteBufAllocator/DEFAULT))
  ([defs ^ByteBufAllocator allocator]
   (s/conform ::struct-def defs)
   (fn [value]
     (let [buffer (.buffer allocator)]
       (doseq [{:keys [name type repeated optional skip] :or {repeated false optional false skip false} :as d} defs]
         (let [v (get value name)
               write-value (fn [value]
                             (case type
                               :int32 (.writeIntLE buffer value)
                               :int64 (.writeLongLE buffer value)
                               :cstring (do
                                          (.writeCharSequence buffer value "UTF-8")
                                          (.writeByte buffer 0))
                               :document (write-bson buffer value)
                               :section (cond
                                          (= [:body] (keys value)) (do
                                                                     (.writeByte buffer 0)
                                                                     (write-bson buffer (:body value)))
                                          (= #{:seq-id :documents} (set (keys value)))
                                          (do
                                            (.writeByte buffer 1)
                                            (.writeIntLE buffer (:len))))))]
           (cond repeated (if (sequential? v)
                            (doseq [v v] (write-value v))
                            (write-value v))
                 optional (when (some? v)
                            (write-value v))
                 skip (when (#{:int32 :int64} type)
                        (write-value 0))
                 :else (if (some? v)
                         (write-value v)
                         (throw (ex-info "non-optional value missing" {:def d}))))))
       buffer))))