(ns active-riemann.common
  "Common functionality."
  (:require [riemann.config :as riemann-config]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [riemann.common :as common]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.tools.logging :as logging]))

(def hostname
  (let [[_age hostname]
        (common/get-hostname nil)]
    hostname))

(defn currentTimeSeconds
  []
  (quot (System/currentTimeMillis) 1000))

(defn currentTime
  []
  (time-coerce/from-long (currentTimeSeconds)))

(defn toTime
  [epoch-sec ms]
  (time-coerce/from-long (long (+ (* 1000 epoch-sec) ms))))

(let [iso-fmt (time-format/formatters :date-time)]
  (defn unix-to-iso8602 [epoch-sec ms]
    (time-format/unparse iso-fmt
                         (toTime epoch-sec ms)))
  (defn current-to-iso8602 []
    (time-format/unparse iso-fmt (currentTime))))

(def timeout-ms 20000)

(defn batch-with-single-retry
  [label batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time
   exception-event->log-msg child-stream]
  (let [singleton-stream
        (riemann-test/io (riemann-config/async-queue!
                          (str label "-singleton")
                          {:queue-size queue-size :core-pool-size core-pool-size
                           :max-pool-size max-pool-size :keep-alive-time keep-alive-time}
                          (bound-fn* child-stream)))
        batch-stream
        (riemann-test/io (riemann-config/async-queue!
                          (str label "-batch")
                          {:queue-size queue-size :core-pool-size core-pool-size
                           :max-pool-size max-pool-size :keep-alive-time keep-alive-time}
                          (bound-fn* (riemann-streams/batch batch-n batch-dt child-stream))))
        batch-with-single-retry
        (riemann-streams/exception-stream
         (fn [batched-exception-event]
           ;; `batched-exception-event` is an exception event that wraps the clj-http exception:
           ;; {:time `unix-time`
           ;;  :service "riemann exception"
           ;;  :state "error"
           ;;  :tags ["exception" (.getName (class e))]
           ;;  :event original
           ;;  :exception e that carriers `ex-data`
           ;;  :description (str e "\n\n"
           ;;                    (join "\n" (.getStackTrace e)))}
           ;; ex-data is from clj-http:
           ;; {:status `status` :headers `map of headers` :body `response body`}
           (let [original-events (:event batched-exception-event)]
             (logging/warn label "failed to forward" (count original-events) "events; trying to submit individually")
             (logging/warn label (exception-event->log-msg batched-exception-event))
             (doseq [ev original-events]
               ((riemann-streams/exception-stream
                 (fn [singleton-exception-event]
                   (let [original-event (:event singleton-exception-event)]
                     (logging/warn label "finally failed to forward singleton event:" (pr-str original-event))
                     (logging/warn label (let [exd (ex-data (:exception singleton-exception-event))] (str (:status exd) " " (pr-str (:body exd)))))))
                 singleton-stream) ev))))
         batch-stream)]
    batch-with-single-retry))
