(ns active-riemann.logging
  "Facilities for logging."
  (:require [active.clojure.logger.event :as event-logger]))

(defmacro trace
  "Log a `message` at :trace level."
  [& messages]
  `(event-logger/log-event! :trace (event-logger/log-msg ~@messages)))

(defmacro debug
  "Log a `message` at :debug level."
  [& messages]
  `(event-logger/log-event! :debug (event-logger/log-msg ~@messages)))

(defmacro info
  "Log a `message` at :info level."
  [& messages]
  `(event-logger/log-event! :info (event-logger/log-msg ~@messages)))

(defmacro warn
  "Log a `message` at :warn level."
  [& messages]
  `(event-logger/log-event! :warn (event-logger/log-msg ~@messages)))

(defmacro error
  "Log a `message` at :error level."
  [& messages]
  `(event-logger/log-event! :error (event-logger/log-msg ~@messages)))
