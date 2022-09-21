(ns active-riemann.streams-test
  (:require [active-riemann.streams :refer :all]
            [riemann.test :as riemann-test]
            [riemann.time.controlled :as time.controlled]
            [clojure.test :refer :all]))

(defmacro test-stream
  [?stream ?input ?expected]
  `(time.controlled/with-controlled-time!
     (time.controlled/reset-time!)
     (riemann-test/test-stream ~?stream ~?input ~?expected)))

(defn make-example-events
  [times events-per-time]
  (vec (mapcat (fn [time]
                 (map (fn [count]
                        {:time time
                         :data count})
                      (range (* time events-per-time) (* (inc time) events-per-time))))
               (range times))))

(deftest t-make-example-events
  (is (= [{:time 0, :data 0}
          {:time 0, :data 1}
          {:time 0, :data 2}
          {:time 1, :data 3}
          {:time 1, :data 4}
          {:time 1, :data 5}]
         (make-example-events 2 3))))

(deftest t-fifo-throttle
  (testing "simple throttle with stream intervals"
    (riemann-test/test-stream-intervals
     (fifo-throttle 4)
     [:a 3 :b 1 :c 2 :d 3]
     [:a :b :c]))

  (testing "simple throttle with test stream"
    (let [events [{:data :a :time 0}
                  {:data :b :time 3}
                  {:data :c :time 4}
                  {:data :d :time 6}
                  {:state "expired" :time 9}]]
      (test-stream
       (fifo-throttle 3)
       events
       (take 3 events))))

  (testing "test unbounded throttle"
    (let [events (make-example-events 4 3)]
      (test-stream
       (fifo-throttle 1)
       events
       (take 3 events))))

  (testing "test bounded throttle"
    (let [events (conj (make-example-events 4 3) {:time 200 :state "expired"})]
      (test-stream
       (fifo-throttle 1 {:max-fifo-size 100})
       events
       (take 4 events))))

  (testing "test bounded throttle that throws away stuff"
    (let [events (conj (make-example-events 4 3) {:time 200 :state "expired"})]
      (test-stream
       (fifo-throttle 1 {:max-fifo-size 3})
       events
       (take 4 events))))

  (testing "test bounded throttle that does not let anything through"
    (let [events (make-example-events 4 3)]
      (test-stream
       (fifo-throttle 1 {:max-fifo-size 0})
       events
       []))))
