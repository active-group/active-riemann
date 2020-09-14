(ns active-riemann.common
  "Common functionality."
  (:require [riemann.config :as riemann-config]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [riemann.common :as common]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.tools.logging :as logging]))

(def hostname (common/localhost))

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

(defn exception-event->ex-data-log-msg
  [exception-event]
  (when-let [exd (ex-data (:exception exception-event))]
    (str "ex-data " (pr-str exd))))

(defn exception-event->exception-log-msg
  [exception-event]
  (when-let [ex (:exception exception-event)]
    (str "exception " (pr-str ex))))

(defn exception-name
  [exception-event]
  (when-let [ex (:exception exception-event)]
    (str "[" (str ex) "]")))

(defn batch-with-single-retry
  [label batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time
   exception-event->log-msg child-stream]
  (let [async-queue!
        (fn [label child-stream]
          (riemann-config/async-queue!
           label
           {:queue-size queue-size :core-pool-size core-pool-size
            :max-pool-size max-pool-size :keep-alive-time keep-alive-time}
           (bound-fn* child-stream)))
        singleton-stream
        (riemann-test/io (async-queue! (str label "-singleton") child-stream))
        batch-stream
        (riemann-test/io (async-queue!
                          (str label "-batch")
                          (riemann-streams/batch batch-n batch-dt child-stream)))
        quotient 10
        batch-n-quotient (quot batch-n quotient)
        batch-10th-stream
        (riemann-test/io (async-queue!
                          (str label "-batch-10th")
                          (riemann-streams/batch batch-n-quotient batch-dt child-stream)))
        batch-with-single-retry
        (riemann-streams/exception-stream
         (fn [batched-exception-event]
           ;; `batched-exception-event` is an exception event that wraps the clj-http exception:
           ;; {:time `unix-time`
           ;;  :service "riemann exception"
           ;;  :state "error"
           ;;  :tags ["exception" (.getName (class e))]
           ;;  :event original
           ;;  :exception e that carries `ex-data`
           ;;  :description (str e "\n\n"
           ;;                    (join "\n" (.getStackTrace e)))}
           ;; ex-data is from clj-http:
           ;; {:status `status` :headers `map of headers` :body `response body`}
           (let [original-events (:event batched-exception-event)]
             (logging/warn label "failed to forward" (count original-events) "events; trying to submit in chunks of" batch-n-quotient (exception-name batched-exception-event))
             (when-let [log-msg (exception-event->log-msg batched-exception-event)]
               (logging/warn label log-msg))
             (doseq [evs (partition batch-n-quotient original-events)]
               (doseq [ev evs]
                 ((riemann-streams/exception-stream
                   (fn [batched-10th-exception-event]
                     (let [original-events (:event batched-10th-exception-event)]
                       (logging/warn label "failed to forward" (count original-events) "events; trying to submit individually" (exception-name batched-10th-exception-event))
                       (when-let [log-msg (exception-event->log-msg batched-10th-exception-event)]
                         (logging/warn label log-msg))
                       (doseq [ev original-events]
                         ((riemann-streams/exception-stream
                           (fn [singleton-exception-event]
                             (let [original-event (:event singleton-exception-event)]
                               (logging/warn label "finally failed to forward singleton event" (exception-name singleton-exception-event) (pr-str original-event))
                               (when-let [log-msg (exception-event->log-msg singleton-exception-event)]
                                 (logging/warn label log-msg))))
                           singleton-stream) ev))))
                   batch-10th-stream) ev)))))
         batch-stream)]
    batch-with-single-retry))
