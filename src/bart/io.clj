(ns bart.io
  (:require [aleph.tcp :as tcp]
            [bart.protocol :as p]
            [manifold.stream :as s]
            [io.pedestal.log :as log])
  (:import [io.netty.channel ChannelPipeline]
           [io.netty.handler.codec LengthFieldBasedFrameDecoder]
           [java.nio ByteOrder]))

(defn wrap-stream
  [stream]
  (let [sink (s/stream)]
    (s/connect (s/map p/encode sink) stream)
    (s/splice sink (s/map p/decode stream))))

(defn wrap-handler
  [handler]
  (fn [stream info]
    (handler (wrap-stream stream) info)))

(defn start-server
  [handler options]
  (tcp/start-server (wrap-handler handler)
                    (assoc options
                      :raw-stream? true
                      :pipeline-transform (fn [^ChannelPipeline pipeline]
                                            (log/debug :task ::pipeline-transform :phase :begin :pipeline pipeline)
                                            (.addBefore pipeline "handler" "length-decoder"
                                                        (LengthFieldBasedFrameDecoder. ByteOrder/LITTLE_ENDIAN
                                                                                       Integer/MAX_VALUE
                                                                                       0
                                                                                       4
                                                                                       -4
                                                                                       0
                                                                                       true))))))

