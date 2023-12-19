(ns active-riemann.streams-test
  (:require [active-riemann.streams :refer :all]
            [riemann.test :as riemann-test]
            [clojure.test :refer :all]))

;; suppress output of metrics logging
(alter-var-root (var active.clojure.logger.metric/log-metric!-internal)
                (constantly (constantly nil)))

(defmacro test-stream
  [?stream ?input ?expected]
  `(riemann-test/with-test-env
     (riemann-test/test-stream ~?stream ~?input ~?expected)))

(defn make-example-events
  [times events-per-time]
  (vec (mapcat (fn [interval count]
                 [{:data count} interval])
               (mapcat (fn [time] (concat [time] (repeat (dec events-per-time) 0))) (repeat times 1))
               (range))))

(deftest t-make-example-events
  (is (= [{:data 0} 1
          {:data 1} 0
          {:data 2} 0
          {:data 3} 1
          {:data 4} 0
          {:data 5} 0]
         (make-example-events 2 3))))

(deftest t-fifo-throttle
  (testing "events arrive slower than throttle"
    (let [events  [{:data 0} 2
                   {:data 1} 2
                   {:data 2} 2
                   {:data 3} 2
                   {:data 4} 2
                   {:data 5} 2]]
      (riemann-test/test-stream-intervals
        (fifo-throttle 1)
        events
        (take-nth 2 events))))
  (testing "events arrive just in time for throttle"
    (let [events  [{:data 0} 1
                   {:data 1} 1
                   {:data 2} 1
                   {:data 3} 1
                   {:data 4} 1
                   {:data 5} 1]]
      (riemann-test/test-stream-intervals
        (fifo-throttle 1)
        events
        (take-nth 2 events))))
  (testing "events arrive twice as fast for throttle"
    (let [events  [{:data 0} 1
                   {:data 1} 0
                   {:data 2} 1
                   {:data 3} 0
                   {:data 4} 1
                   {:data 5} 0]]
      (riemann-test/test-stream-intervals
        (fifo-throttle 1)
        events
        (take 3 (take-nth 2 events))))))


(deftest t-fifo-throttle-bounded
  (testing "test bounded throttle that is sufficient over time"
    (let [events [{:data 0} 1
                  {:data 1} 0
                  {:data 2} 1
                  {:data 3} 0
                  {:data 4} 1
                  {:data 5} 0
                  ;; this is needed to keep the test going
                  {:data "last"} 10]]
      (riemann-test/test-stream-intervals
       (fifo-throttle 1 {:max-fifo-size 100})
       events
       (take-nth 2 events))))
  (testing "test bounded throttle that is not sufficient over time"
    (let [events [{:data 0} 1
                  {:data 1} 0
                  {:data 2} 1
                  {:data 3} 0
                  {:data 4} 1
                  {:data 5} 0
                  ;; this is needed to keep the test going
                  {:data "last"} 0]]
      (riemann-test/test-stream-intervals
       (fifo-throttle 1 {:max-fifo-size 100})
       events
       (take 3 (take-nth 2 events)))))
  (testing "test bounded throttle that throws elemts away"
    (let [events [{:data 0} 1
                  {:data 1} 0
                  {:data 2} 1
                  {:data 3} 0
                  {:data 4} 1
                  {:data 5} 0
                  ;; this is needed to keep the test going
                  {:data "last"} 10]]
      (riemann-test/test-stream-intervals
       (fifo-throttle 1 {:max-fifo-size 1})
       events
       [{:data 0} {:data 1} {:data 3} {:data 5}])))
  (testing "test bounded throttle that does not let anything through"
    (let [events [{:data 0} 1
                  {:data 1} 0
                  {:data 2} 1
                  {:data 3} 0
                  {:data 4} 1
                  {:data 5} 0
                  ;; this is needed to keep the test going
                  {:data "last"} 10]]
      (riemann-test/test-stream-intervals
       (fifo-throttle 1 {:max-fifo-size 0})
       events
       []))))

(deftest t-fifo-throttle-queue-shrinks-again
  (let [cnt 2000
        events (concat
                (mapcat (fn [cnt] [{:data cnt} 0])
                        (range cnt))
                [{:data "last"} cnt])
        queue-metric (atom nil)]
    (with-redefs [active.clojure.logger.metric/log-metric!-internal
                  (fn [_namespace metric _labels value & [_mp]]
                    (when (active.clojure.logger.metric-accumulator/gauge-metric? metric)
                      (reset! queue-metric value)))]
      (riemann-test/test-stream-intervals
        (fifo-throttle 1 {:max-fifo-size (inc cnt)})
        events
        (take-nth 2 events))
      (is (= 1 @queue-metric)))))
