(ns active-riemann.control
  "Control Riemann instance."
  (:require [riemann.bin :as riemann-bin]
            [riemann.config :as riemann-config]
            [riemann.repl :as riemann-repl]
            [riemann.logging :as riemann-logging]))

(defn default-configure-riemann
  []
  (riemann-logging/init {:console? true})
  (riemann-repl/start-server! {})
  (riemann-config/tcp-server {}))

(defn start-riemann!
  [& [configure-riemann]]
  (riemann-bin/run-app! (or configure-riemann default-configure-riemann)))

(defn add-streams!
  [& things]
  (reset! riemann-config/next-core (update @riemann-config/core :streams #(reduce conj % things)))
  (riemann-config/apply!)
  ;; suppress core as return value
  nil)

(defn remove-streams!
  [& things]
  (reset! riemann-config/next-core (update @riemann-config/core :streams #(remove (fn [s] (some (fn [t] (= s t)) things)) %)))
  (riemann-config/apply!)
  ;; suppress core as return value
  nil)
