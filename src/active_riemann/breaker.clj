(ns active-riemann.breaker
  "Circuit breaker for riemann."
  (:require [riemann.streams :as riemann-streams]
            [riemann.folds :as riemann-folds]
            [active-riemann.logging :as logging]
            [diehard.circuit-breaker :as diehard-circuit-breaker]
            [diehard.core :as diehard-core]
            [active.clojure.record :refer [define-record-type]]
            [active-riemann.common :as common]))

(def riemann-netty-event-executor-queue-size-service-name
  "riemann netty event-executor queue size")

(def riemann-netty-event-executor-queue-size-load-limit
  2000)

(def load-atom (atom {}))

(def load-level-hit-key ::load-level-hit?)

(defn make-indicate-fn
  [load-atom load-level]
  (fn [load-level-hit?]
    (fn stream [_metric-event]
      (logging/info "Load indicator" [load-level load-level-hit-key] load-level-hit?)
      (swap! load-atom assoc-in [load-level load-level-hit-key] load-level-hit?))))

(defn load-level-hit-fn?
  [load-atom load-level]
  (fn []
    (get-in @load-atom [load-level load-level-hit-key])))

(defn load-indicator
  [service-name load-limit failure-duration-seconds indicate-fn & children]
  (riemann-streams/where (service service-name)
                         (riemann-streams/where (= common/hostname (:host event))
                                                (riemann-streams/moving-time-window failure-duration-seconds
                                                                                    (apply riemann-streams/smap
                                                                                           riemann-folds/mean
                                                                                           (riemann-streams/changed #(>= (:metric %) load-limit)
                                                                                                                    (riemann-streams/where (>= metric load-limit)
                                                                                                                                           (indicate-fn true)
                                                                                                                                           (else
                                                                                                                                            (indicate-fn false))))
                                                                                           children)))))

(defn riemann-load-indicator
  [load-atom load-level failure-duration-minutes & args]
  (let [opts-map (if (map? (first args)) (first args) {})
        children (if (map? (first args)) (rest args) args)
        {:keys [indicator-service-name indicator-metric-limit]
         :or {indicator-service-name riemann-netty-event-executor-queue-size-service-name
              indicator-metric-limit riemann-netty-event-executor-queue-size-load-limit}} opts-map]
    (apply load-indicator
           indicator-service-name
           indicator-metric-limit
           (* 60 failure-duration-minutes)
           (make-indicate-fn load-atom load-level)
           children)))

(defn riemann-circuit-breaker
  [label load-level-hit-fn? resume-delay-minutes & [opts-map]]
  (diehard-circuit-breaker/circuit-breaker (merge {:delay-ms (* 60 1000 resume-delay-minutes)
                                                   :failure-threshold 1
                                                   :success-threshold 1
                                                   :fail-if (fn [_result _exception] (load-level-hit-fn?))
                                                   :on-open (fn [] (logging/info "Circuit breaker" label "opened"))
                                                   :on-half-open (fn [] (logging/info "Circuit breaker" label "half-opened"))
                                                   :on-close (fn [] (logging/info "Circuit breaker" label "closed"))}
                                                  opts-map)))

(defn with-riemann-circuit-breaker
  [circuit-breaker & children]
  (fn stream [event]
    (try
      (diehard-core/with-circuit-breaker circuit-breaker
        (riemann-streams/call-rescue event children))
      (catch net.jodah.failsafe.CircuitBreakerOpenException _e
        (riemann-streams/sdo)))))

(define-record-type Breaker
  make-breaker
  breaker?
  [indicator-stream breaker-indicator-stream
   breaker-stream breaker-breaker-stream])

(defn make-indicator-stream-and-breaker-stream
  [load-level failure-duration-minutes resume-delay-minutes & [opts-map]]
  (make-breaker
   (partial riemann-load-indicator load-atom load-level failure-duration-minutes (or opts-map {}))
   (partial with-riemann-circuit-breaker (riemann-circuit-breaker load-level (load-level-hit-fn? load-atom load-level) resume-delay-minutes))))

;; Load levels on system:
;; mild, moderate, elevated, severe, extreme
;; depending on time the failure condition is hit
(def load-level-definitions
  {:mild     {:failure-duration-minutes  5 :resume-delay-minutes 10}
   :moderate {:failure-duration-minutes 10 :resume-delay-minutes 20}
   :elevated {:failure-duration-minutes 15 :resume-delay-minutes 30}
   :severe   {:failure-duration-minutes 20 :resume-delay-minutes 40}
   :extreme  {:failure-duration-minutes 30 :resume-delay-minutes 60}})

(defn make-breakers
  ([]
   (make-breakers load-level-definitions))
  ([load-level-definitions & [opts-map]]
   (reduce (fn [r [load-level {:keys [failure-duration-minutes resume-delay-minutes]}]]
             (assoc r load-level (make-indicator-stream-and-breaker-stream load-level failure-duration-minutes resume-delay-minutes opts-map)))
           {} load-level-definitions)))

(defmacro define-breaker
  [level failure-duration-minutes resume-delay-minutes indicator-stream-binding breaker-stream-binding & [opts-map]]
  (let [breaker `breaker#]
  `(let [~breaker (make-indicator-stream-and-breaker-stream ~level ~failure-duration-minutes ~resume-delay-minutes (or ~opts-map {}))]
     (def ~indicator-stream-binding (breaker-indicator-stream ~breaker))
     (def ~breaker-stream-binding (breaker-breaker-stream ~breaker)))))
