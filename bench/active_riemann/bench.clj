(ns active-riemann.bench
  (:require [active-riemann.index :as active-index]
            [active-riemann.logging :as log]
            [active.clojure.config :as active-config]
            [active.clojure.logger.event :as event-logger]
            [active.clojure.logger.timbre :as timbre]
            [riemann.config :as riemann-config]
            [riemann.index :as riemann-index]
            [criterium.core :as crit])
  (:import (java.io File)))

;; File path helper

(defn base-file-path
  [datetime file-ending]
  (str "/tmp/active-riemann-benchmark-"
       (java.time.LocalDateTime/.getYear datetime) "-"
       (format "%02d" (java.time.LocalDateTime/.getMonthValue datetime)) "-"
       (format "%02d" (java.time.LocalDateTime/.getDayOfMonth datetime)) "-"
       (format "%02d" (java.time.LocalDateTime/.getHour datetime)) "-"
       (format "%02d" (java.time.LocalDateTime/.getMinute datetime)) "-"
       (format "%02d" (java.time.LocalDateTime/.getSecond datetime))
       file-ending))

;; Logging

(defn log-config
  [_log-file-path]
  ;; (println "Log file-path: " log-file-path)
  (active-config/make-configuration
   timbre/timbre-config-schema
   []
   ;; note: using criterium with
   ;; :min-level :info and spit to a log-file (here in /tmp)
   ;; can fill up your /tmp-folder
   {:min-level :error
    ;; :appenders
    ;; {:spit (list 'spit {:fname log-file-path})}
    }))

;; Event creation

(defn random-key-values
  [n]
  (mapv (fn [_]
          {(keyword (str "keyword-" (rand-int Integer/MAX_VALUE)))
           (str "value-" (rand-int Integer/MAX_VALUE))})
        (range 0 n)))

(defn random-events
  [number-of-events number-of-event-key-values]
  (mapv (fn [_]
          (merge
           {:host (str "host-" (rand-int Integer/MAX_VALUE))
            :service (str "service-" (rand-int Integer/MAX_VALUE))
            :time (rand-int Integer/MAX_VALUE)}
           (reduce into {} (random-key-values number-of-event-key-values))))

        (range 0 number-of-events)))

;; bench cases

;; -- backup

(defn backup-main
  [number-of-events number-of-event-key-values store-file-path dry-run?]
  (when-not dry-run? (println "Prepare backup benchmark"))
  (let [the-index (riemann-config/index)
        events (random-events number-of-events number-of-event-key-values)]
    (when-not dry-run?
      (println "Number of events: " number-of-events)
      (println "Number of key-values per event: " number-of-event-key-values)
      (println "Insert events in index..."))

    (mapv #(riemann-index/insert the-index %) events)

    (when-not dry-run? (println "Start benchmark"))
    (let [run #(active-index/backup-index the-index store-file-path)]
      (if (not dry-run?)
        (crit/bench (run))
        (run)))
    (when-not dry-run? (println "End benchmark"))))

;; -- restore

;; >>> enhanced but costly insertion

(defn- riemann-index-search
  [index query]
  (log/debug `riemann-index-search "calling 'riemann.index/search' with" query)
  (riemann-index/search index query))

(defn more-recent-event-query
  "riemann query ast to ask the riemann-index for an event, that has the same
  :host and :service as the provided one, but with a more recent :time."
  [event]
  (list 'and
        (list 'and
              (list '= ':host (:host event))
              (list '= ':service (:service event)))
        (list '>= ':time (:time event))))

;; NOTE: this is costly due to the `:time` in the query
(defn enhanced-insert?
  [index event]
  (if (and (:host event) (:service event))
    (empty? (riemann-index-search index (more-recent-event-query event)))
    false))

;; <<< enhanced but costly insertion

(defn f-insertion-approval
  [insertion-approval]
  (cond
    (= insertion-approval :dummy) active-index/always-insert
    (= insertion-approval :util) active-index/util-insert?
    (= insertion-approval :enhanced) enhanced-insert?))

(defn restore-main
  [insertion-approval number-of-events number-of-event-key-values dry-run?]
  (when-not dry-run? (println "Prepare benchmark"))
  (let [events (random-events number-of-events number-of-event-key-values)]
    (when-not dry-run?
      (println "Number of events: " number-of-events)
      (println "Number of key-values per event: " number-of-event-key-values)
      (println "Insertion-approval: " insertion-approval))

    (when-not dry-run? (println "Start benchmark"))
    (let [run #(active-index/insert-events (riemann-config/index)
                                           events
                                           (f-insertion-approval insertion-approval))]
      (if (not dry-run?)
        (crit/bench (run))
        (run)))
    
    (when-not dry-run? (println "End benchmark"))))

;; main
;; args:
;; "{:task :backup  :number-of-events <number> :number-of-event-key-values <number>}"
;; "{:task :restore :number-of-events <number> :number-of-event-key-values <number>
;;                  :copy-or-running <:copy|:running>
;;                  :insertion-approval <:dummy|:util|:enhanced>}
(defn -main
  [& args]
  (let [setup-values (read-string (nth args 0))
        dry-run? (= "--dry-run" (when (> (count args) 1) (nth args 1)))
        datetime (java.time.LocalDateTime/now)]
    
    (-> (log-config nil #_(base-file-path datetime ".log"))
        timbre/configuration->timbre-config
        event-logger/set-global-log-events-config!)

    (let [store-file-path (if dry-run?
                            (.getPath (doto (File/createTempFile "active-riemann-benchmark-" ".edn")
                                        (.deleteOnExit)))
                            (base-file-path datetime ".edn"))]

      (cond
        (= (:task setup-values) :backup)
        (backup-main (:number-of-events setup-values)
                     (:number-of-event-key-values setup-values)
                     store-file-path
                     dry-run?)
        (= (:task setup-values) :restore)
        (restore-main (:f-insertion? setup-values)
                      (:number-of-events setup-values)
                      (:number-of-event-key-values setup-values)
                      dry-run?)))))
