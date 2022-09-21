(ns active-riemann.streams
  "Additional streams functionality"
  (:require [riemann.streams :as riemann-streams]))

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
        max-fifo-size (:max-fifo-size options)]
    (riemann-streams/part-time-simple dt
      ; Copy the previously arrived elements or initialize queue
      (fn reset [queue] (if (nil? queue) [] (vec (rest queue))))

      ; Conj new elements into the queue
      (fn add [queue event]
        (if (and (number? max-fifo-size) (>= (count queue) max-fifo-size))
          ;; discard incoming event if max queue length is hit
          queue
          (conj queue event)))

      ; Do nothing when event arrives, only when the time interval has elapsed
      (constantly nil)

      ; Send elements once the time interval has elapsed
      (fn flush [queue _start-time _end-time]
        (when-let [event (and (vector? queue) (first queue))]
          (riemann-streams/call-rescue event children))))))
