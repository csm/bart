(ns bart.core
  "S.B.: Excuse me while I whip this out."
  (:require [bart.io :as bio]
            [io.pedestal.log :as log]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:gen-class)
  (:import [java.net InetSocketAddress]))

(defn handler
  [stream info]
  (log/info :task ::handler :phase :stream-connected :info info)
  (s/on-closed stream
               (fn []
                 (log/info :task ::handler :phase :stream-closed :info info)))
  (d/loop []
    (d/chain
      (s/take! stream)
      (fn [message]
        (log/debug :task ::handler :phase :handle-message :message message)
        (if (some? message)
          (d/recur)
          (s/close! stream))))))

(defn -main
  [& args]
  (let [server (bio/start-server handler {:socket-address (InetSocketAddress. "127.0.0.1" 0)})]
    (log/info :task ::main :phase :started :port (aleph.netty/port server))
    @(promise)))