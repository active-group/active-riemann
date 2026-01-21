(ns active-riemann.simulator
  "Run Riemann for interactive tests and simulation."
  (:require [riemann.config :as riemann-config]
            [riemann.client :as riemann-client]
            [riemann.streams :as riemann-streams]
            [active-riemann.control :as control]
            [active-riemann.breaker :as breaker]
            [active-riemann.logging :as logging]))

(defonce riemann-client-atom (atom nil))

(defn make-test-event
  [& stuff]
  (merge
   {:host "riemann.local" :service "active-riemann test" :description "test event" :time (/ (System/currentTimeMillis) 1000)}
   stuff))

(defn connect-client!
  []
  (let [riemann-client (riemann-client/tcp-client)]
    (riemann-client/connect! riemann-client)
    (reset! riemann-client-atom riemann-client)
    riemann-client))

(defn send-event!
  ([]
   (send-event! (make-test-event)))
  ([event]
   (send-event! @riemann-client-atom event))
  ([riemann-client event]
   (deref (riemann-client/send-event riemann-client event))))

(defn send-events!
  ([events]
   (send-events! @riemann-client-atom events))
  ([riemann-client events]
   (deref (riemann-client/send-events riemann-client events))))

(defn send-generated-events!
  [n]
  (future
    (dotimes [_n n]
      (send-event!))))

(defn fire-event!
  ([]
   (fire-event! (make-test-event)))
  ([event]
   (fire-event! @riemann-client-atom event))
  ([riemann-client event]
   (riemann-client/send-event riemann-client event)))

(defn fire-events!
  ([events]
   (fire-events! @riemann-client-atom events))
  ([riemann-client events]
   (riemann-client/send-events riemann-client events)))

(defn fire-generated-events!
  [n]
  (future
    (dotimes [_n n]
      (fire-event!))))

(defn reinject-event!
  ([]
   (reinject-event! (make-test-event)))
  ([event]
   (riemann-config/reinject event)))

(defn reinject-generated-events!
  [n]
  (future
    (dotimes [_n n]
      (reinject-event!))))

(defn info-stream
  [e]
  (logging/info (pr-str e)))

(defn delay-stream
  [delay-seconds]
  (fn [_e]
    (Thread/sleep (* 1000 delay-seconds))))

(defn a-delay-stream
  [a-delay]
  (fn [_e]
    (Thread/sleep @a-delay)))

(def log-netty-metric-stream
  (riemann-streams/where (service "riemann netty event-executor queue size")
                         info-stream))

(def log-tcp-in-rate-stream
  (riemann-streams/where (service "riemann server tcp 127.0.0.1:5555 in rate")
                         info-stream))

(defonce a-delay (atom 0))

(def the-a-delay-stream (a-delay-stream a-delay))

(def breakers (breaker/make-breakers {:test {:failure-duration-minutes 1 :resume-delay-minutes 2}} {:indicator-metric-limit 2000000}))
(def test-breaker (:test breakers))
(def test-breaker-indicator-stream (active-riemann.breaker/breaker-indicator-stream test-breaker))
(def test-breaker-breaker-stream (active-riemann.breaker/breaker-breaker-stream test-breaker))
(def breaker-indicator-stream (test-breaker-indicator-stream))
(def breaker-delay-stream (test-breaker-breaker-stream the-a-delay-stream))

(defn -main
  [& _args]
  (control/start-riemann!)
  (control/add-streams! log-netty-metric-stream breaker-indicator-stream breaker-delay-stream)
  (reset! a-delay 20))
