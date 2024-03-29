(ns active-riemann.breaker-test
  (:require [active-riemann.breaker :refer :all]
            [active-riemann.common :as common]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [clojure.test :as test]))

;; suppress output of logging
(alter-var-root (var active.clojure.logger.internal/log-event!-internal)
                (constantly (constantly nil)))

(test/deftest t-load-indicator
  (test/testing "One event with high metric."
    (riemann-test/test-stream
     (load-indicator "test-service" 10 60 (fn [res] (fn [_metric-event] (test/is (true? res)))))
     [{:service "test-service" :host common/hostname :metric 100}]
     [{:service "test-service" :host common/hostname :metric 100}]))
  (test/testing "One event with low metric."
    (riemann-test/test-stream
     (load-indicator "test-service" 10 60 (fn [res] (fn [_metric-event] (test/is (false? res)))))
     [{:service "test-service" :host common/hostname :metric 1}]
     [{:service "test-service" :host common/hostname :metric 1}]))
  (test/testing "Many events with high metric."
    (riemann-test/test-stream
     (load-indicator "test-service" 10 60 (fn [res] (fn [_metric-event] (test/is (true? res)))))
     [{:service "test-service" :host common/hostname :metric 100}
      {:service "test-service" :host common/hostname :metric 100}
      {:service "test-service" :host common/hostname :metric 10}]
     [{:service "test-service" :host common/hostname :metric 100}
      {:service "test-service" :host common/hostname :metric 100}
      {:service "test-service" :host common/hostname :metric 70}]))
  (test/testing "Many events with low metric."
    (riemann-test/test-stream
     (load-indicator "test-service" 10 60 (fn [res] (fn [_metric-event] (test/is (false? res)))))
     [{:service "test-service" :host common/hostname :metric 1}
      {:service "test-service" :host common/hostname :metric 10}
      {:service "test-service" :host common/hostname :metric 10}]
     [{:service "test-service" :host common/hostname :metric 1}
      {:service "test-service" :host common/hostname :metric 11/2}
      {:service "test-service" :host common/hostname :metric 7}])))

(test/deftest t-riemann-load-indicator
  (test/testing "One event with high metric."
    (riemann-test/run-stream
     (riemann-load-indicator load-atom :test-level 1)
     [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}])
    (test/is (true? ((load-level-hit-fn? load-atom :test-level)))))
  (test/testing "One event with low metric."
    (riemann-test/run-stream
     (riemann-load-indicator load-atom :test-level 1)
     [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}])
    (test/is (false? ((load-level-hit-fn? load-atom :test-level)))))
  (test/testing "Many events with high metric."
    (riemann-test/run-stream
     (riemann-load-indicator load-atom :test-level 1)
     [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}])
    (test/is (true? ((load-level-hit-fn? load-atom :test-level)))))
  (test/testing "Many events with low metric."
    (riemann-test/run-stream
     (riemann-load-indicator load-atom :test-level 1)
     [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}])
    (test/is (false? ((load-level-hit-fn? load-atom :test-level))))))

(test/deftest t-with-riemann-circuit-breaker
  (test/testing "Circuit breaker closed."
    (let [circuit-breaker (riemann-circuit-breaker :test-level (load-level-hit-fn? load-atom :test-level) 1)]
      (riemann-test/test-stream
       (riemann-streams/sdo
        ((make-indicate-fn load-atom :test-level) false)
        (with-riemann-circuit-breaker circuit-breaker))
       [{:service "test-event"}]
       [{:service "test-event"}])))
  (test/testing "Circuit breaker opened."
    (let [circuit-breaker (riemann-circuit-breaker :test-level (load-level-hit-fn? load-atom :test-level) 1)]
      (((make-indicate-fn load-atom :test-level) true) 'event)
      (riemann-test/test-stream
       (with-riemann-circuit-breaker circuit-breaker)
       [{:service "test-event"}{:service "test-event"}]
       [{:service "test-event"}])
      (riemann-test/test-stream
       (with-riemann-circuit-breaker circuit-breaker)
       [{:service "test-event"}{:service "test-event"}]
       [])))
  (test/testing "Circuit breaker closed again."
    (let [circuit-breaker (riemann-circuit-breaker :test-level (load-level-hit-fn? load-atom :test-level) 1)]
      (((make-indicate-fn load-atom :test-level) true) 'event)
      (riemann-test/test-stream
       (with-riemann-circuit-breaker circuit-breaker)
       [{:service "test-event"}{:service "test-event"}]
       [{:service "test-event"}])
      (((make-indicate-fn load-atom :test-level) false) 'event)
      (.close circuit-breaker)
      (riemann-test/test-stream
       (with-riemann-circuit-breaker circuit-breaker)
       [{:service "test-event"}{:service "test-event"}]
       [{:service "test-event"}{:service "test-event"}]))))

(test/deftest t-make-indicator-stream-and-breaker-stream
  (let [breaker (make-indicator-stream-and-breaker-stream :t-make-indicator-stream-and-breaker-stream 1 1)
        indicator-stream (active-riemann.breaker/breaker-indicator-stream breaker)
        breaker-stream (active-riemann.breaker/breaker-breaker-stream breaker)]
    (test/testing "indicator stream"
      (riemann-test/run-stream
       (indicator-stream)
       [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
        {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
        {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}])
      (test/is (true? ((load-level-hit-fn? load-atom :t-make-indicator-stream-and-breaker-stream)))))
    (test/testing "breaker stream"
      (riemann-test/test-stream
       (breaker-stream)
       [{:service "test-event"}{:service "test-event"}]
       [{:service "test-event"}]))))

(test/deftest t-define-breaker
  (define-breaker :t-define-breaker 1 1 indicator-stream breaker-stream)
  (test/testing "indicator stream"
    (riemann-test/run-stream
     (riemann-streams/sdo (indicator-stream))
     [{:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (* riemann-netty-event-executor-queue-size-load-limit 2)}
      {:service riemann-netty-event-executor-queue-size-service-name :host common/hostname :metric (/ riemann-netty-event-executor-queue-size-load-limit 2)}])
    (test/is (true? ((load-level-hit-fn? load-atom :t-define-breaker)))))
  (test/testing "breaker stream"
    (riemann-test/test-stream
     (breaker-stream)
     [{:service "test-event"}{:service "test-event"}]
     [{:service "test-event"}])))
