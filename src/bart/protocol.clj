(ns bart.protocol
  (:require [bart.struct :as bs]
            [clojure.set :refer [map-invert]]
            [io.pedestal.log :as log])
  (:import [io.netty.buffer ByteBuf Unpooled]))

(def opcodes
  {:opcode/reply 1
   :opcode/update 2001
   :opcode/insert 2002
   :opcode/query 2004
   :opcode/get-more 2005
   :opcode/delete 2006
   :opcode/kill-cursors 2007
   :opcode/command 2010
   :opcode/command-reply 2011
   :opcode/msg 2013})

(def message-header [{:name :length :type :int32}
                     {:name :request-id :type :int32}
                     {:name :response-to :type :int32}
                     {:name :opcode :type :int32}])

(def op-update [{:name :reserved :skip true :type :int32}
                {:name :collection :type :cstring}
                {:name :flags :type :int32}
                {:name :selector :type :document}
                {:name :update :type :document}])

(def op-insert [{:name :flags :type :int32}
                {:name :collection :type :cstring}
                {:name :documents :type :document :repeated true}])

(def op-query [{:name :flags :type :int32}
               {:name :collection :type :cstring}
               {:name :skip :type :int32}
               {:name :return :type :int32}
               {:name :query :type :document}
               {:name :selector :type :document :optional true}])

(def op-get-more [{:name :reserved :type :int32 :skip true}
                  {:name :collection :type :cstring}
                  {:name :return :type :int32}
                  {:name :cursor :type :int64}])

(def op-delete [{:name :reserved :type :int32 :skip true}
                {:name :collection :type :cstring}
                {:name :flags :type :int32}
                {:name :selector :type :document}])

(def op-kill-cursors [{:name :reserved :type :int32 :skip true}
                      {:name :count :type :int32}
                      {:name :cursors :type :int64 :repeated true}])

(def op-reply [{:name :flags :type :int32}
               {:name :cursor :type :int64}
               {:name :start :type :int32}
               {:name :returned :type :int32}
               {:name :documents :type :document :repeated true}])

; not implementing op-command, op-command-reply

(def op-msg [{:name :flags :type :int32}
             {:name :sections :type :section :repeated true}
             {:name :checksum :type :int32 :optional true}])

(let [decode-header (bs/struct-reader message-header)
      decode-update (bs/struct-reader op-update)
      decode-insert (bs/struct-reader op-insert)
      decode-query (bs/struct-reader op-query)
      decode-get-more (bs/struct-reader op-get-more)
      decode-delete (bs/struct-reader op-delete)
      decode-kill-cursors (bs/struct-reader op-kill-cursors)
      decode-msg (bs/struct-reader op-msg)]
  (defn decode
    [^ByteBuf buffer]
    (log/debug :task ::decode :phase :begin :buffer buffer)
    (let [header (decode-header buffer)
          opcode (get (map-invert opcodes) (:opcode header))
          body (case opcode
                 :opcode/update (decode-update buffer)
                 :opcode/insert (decode-insert buffer)
                 :opcode/query (decode-query buffer)
                 :opcode/get-more (decode-get-more buffer)
                 :opcode/delete (decode-delete buffer)
                 :opcode/kill-cursors (decode-kill-cursors buffer)
                 :opcode/msg (decode-msg buffer))]
      (.release buffer)
      (let [message (assoc header :body body :opcode opcode)]
        (log/debug :task ::decode :phase :end :message message)
        message))))

(let [encode-header (bs/struct-writer message-header)
      encode-reply (bs/struct-writer op-reply)
      encode-msg (bs/struct-writer op-msg)]
  (defn encode
    [msg]
    (log/debug :task :encode :phase :begin :msg msg)
    (let [body-buf ^ByteBuf (case (:opcode msg)
                              :opcode/reply (encode-reply (:body msg))
                              :opcode/msg (encode-msg (:body msg)))
          header-buf (encode-header (-> (assoc msg :length (+ 16 (.readableBytes body-buf)))
                                        (update :opcode opcodes)))
          buffer (Unpooled/wrappedBuffer (into-array ByteBuf [header-buf body-buf]))]
      (log/debug :task ::encode :phase :end :buffer buffer)
      buffer)))