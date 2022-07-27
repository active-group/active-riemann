(ns active-riemann.streams
  "Additional streams functionality"
  (:require [riemann.streams :as riemann-streams]))

(defn fifo-throttle
  "Passes on one event every `dt` seconds.  Imposes additional latency.
  The internal queue grows unrestrictedly if arriving events outpace leaving
  events."
  [dt & children]
  (riemann-streams/part-time-simple dt
    ; Copy the previously arrived elements or initialize queue
    (fn reset [queue] (if (nil? queue) [] (vec (rest queue))))

    ; Conj new elements into the queue
    (fn add [queue event]
      (conj queue event))

    ; Do nothing when event arrives, only when the time interval has elapsed
    (constantly nil)

    ; Send elements once the time interval has elapsed
    (fn flush [queue _start-time _end-time]
      (when (vector? queue)
        (riemann-streams/call-rescue (first queue) children)))))
