(defproject bart "0.1.0-SNAPSHOT"
  :description "A MongoDB compatible wrapper around Amazon DynamoDB"
  :url "https://github.com/csm/bart"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [aleph "0.4.6"]
                 [com.cognitect.aws/api "0.8.243"]
                 [com.cognitect.aws/endpoints "1.1.11.490"]
                 [com.cognitect.aws/dynamodb "697.2.391.0"]
                 [org.mongodb/bson "3.10.0"]
                 [io.pedestal/pedestal.log "0.5.5"]
                 [ch.qos.logback/logback-classic "1.1.8"]
                 [ch.qos.logback/logback-core "1.1.8"]]
  :main ^:skip-aot bart.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
