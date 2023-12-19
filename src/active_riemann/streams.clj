(ns active-riemann.streams
  "Additional streams functionality"
  (:require [riemann.streams :as riemann-streams]
            [riemann.time :as riemann-time]
            [active.clojure.logger.metric :as metric]))

(defn fifo-throttle
  "Passes on one event every `dt` seconds.  Imposes additional latency.
  The internal queue grows unrestrictedly if arriving events outpace leaving
  events and if `:max-fifo-size` is not set.  Options:

  :max-fifo-size   The maxiumum size of the queue, arriving events are
                   discarded when queue is at maximum capactiy."
  [dt & children]
  (let [options (if (map? (first children)) (first children) {})
        children (if (map? (first children))
                   (rest children)
                   children)
        max-fifo-size (:max-fifo-size options)
        metric-labels-fn (or (:metric-labels-fn options) (constantly {}))

        add
        (fn add [queue event]
          (let [queue-count (count queue)
                metric-labels (metric-labels-fn event)]
            (metric/log-counter-metric! "active_riemann_streams_fifo_throttle_events_total" metric-labels 1)
            (if (and (number? max-fifo-size) (>= queue-count max-fifo-size))
              ;; discard incoming event if max queue length is hit
              (do
                (metric/log-counter-metric! "active_riemann_streams_fifo_throttle_discarded_events_total" metric-labels 1)
                queue)
              (do
                (metric/log-gauge-metric! "active_riemann_streams_fifo_queue_size" metric-labels (inc queue-count))
                (conj queue event)))))

        remove
        (fn remove [queue]
          (vec (rest queue)))

        flush
        (fn flush [queue]
          (when-let [event (first queue)]
            (riemann-streams/call-rescue event children)))

        a-queue (atom [])

        enqueue!
        (fn enqueue [event]
          (swap! a-queue add event))

        periodically-dequeue!
        (fn periodically-dequeue []
          (swap! a-queue (fn [queue]
                           (flush queue)
                           (remove queue))))]
    (riemann-time/every! dt periodically-dequeue!)
    enqueue!))
