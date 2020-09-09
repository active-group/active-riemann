(ns active-riemann.breaker
  "Circuit breaker for riemann."
  (:require [riemann.streams :as riemann-streams]
            [riemann.folds :as riemann-folds]
            [clojure.tools.logging :as logging]
            [diehard.circuit-breaker :as diehard-circuit-breaker]
            [diehard.core :as diehard-core]
            [active.clojure.record :refer [define-record-type]]))

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
      (swap! load-atom assoc-in [load-level load-level-hit-key] load-level-hit?))))

(defn load-level-hit-fn?
  [load-atom load-level]
  (fn []
    (get-in @load-atom [load-level load-level-hit-key])))

(defn load-indicator
  [service-name load-limit indicate-fn & args]
  (let [[opts-map & children] (if (map? (first args))
                                args
                                (concat [{}] args))
        {:keys [n-seconds]
         :or {n-seconds 600}} opts-map]
    (riemann-streams/where (service service-name)
                           (riemann-streams/by :host
                                               (riemann-streams/moving-time-window n-seconds
                                                                                   (apply riemann-streams/smap
                                                                                          riemann-folds/mean
                                                                                          (riemann-streams/where (>= metric load-limit)
                                                                                                                 (indicate-fn true)
                                                                                                                 (else
                                                                                                                  (indicate-fn false)))
                                                                                          children))))))

(defn riemann-load-indicator
  [load-atom load-level failure-duration-minutes & args]
  (let [[opts-map & children] (if (map? (first args))
                                args
                                (concat [{}] args))]
    (load-indicator riemann-netty-event-executor-queue-size-service-name
                    riemann-netty-event-executor-queue-size-load-limit
                    (make-indicate-fn load-atom load-level)
                    (merge opts-map
                           {:n-seconds (* 60 failure-duration-minutes)})
                    children)))

(defn riemann-circuit-breaker
  [label load-level-hit-fn? resume-delay-minutes]
  (diehard-circuit-breaker/circuit-breaker {:delay-ms (* 60 1000 resume-delay-minutes)
                                            :failure-threshold 1
                                            :success-threshold 1
                                            :fail-if (fn [_result _exception] (load-level-hit-fn?))
                                            :on-open (fn [] (logging/info "Circuit breaker" label "opened"))
                                            :on-half-open (fn [] (logging/info "Circuit breaker" label "half-opened"))
                                            :on-close (fn [] (logging/info "Circuit breaker" label "closed"))}))

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
  [load-level failure-duration-minutes resume-delay-minutes]
  (make-breaker
   (riemann-load-indicator load-atom load-level failure-duration-minutes)
   (with-riemann-circuit-breaker (riemann-circuit-breaker load-level (load-level-hit-fn? load-atom load-level) resume-delay-minutes))))

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
  ([load-level-definitions]
   (reduce (fn [r [load-level {:keys [failure-duration-minutes resume-delay-minutes]}]]
             (assoc r load-level (make-indicator-stream-and-breaker-stream load-level failure-duration-minutes resume-delay-minutes)))
           {} load-level-definitions)))
