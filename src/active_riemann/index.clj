(ns active-riemann.index
  "Functionality related to the Riemann index."
  (:require [clojure.edn :as clojure-edn]
            [active-riemann.logging :as log]
            [riemann.index :as riemann-index]))

;; TODOs
;; - Rename restore-index (we add events to an existing index, it is not a real restore)
;; - only inject events if no 'more recent event' is available
;;   (riemann-index/lookup this host service) - Lookup an indexed event from the index
;;   (riemann-index/search this query-ast) - Returns a seq of events from the index matching this query AST
;; - adjust events to inject: optional function event -> event

;; backup index

(defn try-store
  [path-to-file data]
  (try (spit path-to-file (or data []) :append false)
       (log/info `try-store-edn "Successfully stored riemann-index backup file:" path-to-file)
       data
       (catch java.lang.IllegalArgumentException e
         (log/error `try-store-edn "no riemann-index backup file specified:" path-to-file (pr-str e)))
       (catch Exception e
         (log/error `try-store-edn "Error writing riemann-index backup file:" (pr-str e)))))

(defn index->vector
  [index]
  (mapv identity (riemann-index/search index "*")))

(defn backup-index
  [index index-backup-file-path]
  (try-store index-backup-file-path (index->vector index)))

;; add events to index

(defn try-open-edn
  [path-to-edn-file]
  (try
    (let [edn (clojure-edn/read-string (slurp path-to-edn-file))]
      (if (or (nil? edn) (empty? edn))
        []
        edn))
    (catch java.io.FileNotFoundException _e
      (log/error `try-open-edn "riemann-index backup file not found:" path-to-edn-file))
    (catch Exception e
      (log/error `try-open-edn "Error reading riemann-index backup file:" path-to-edn-file (.getMessage e)))))

(defn inject-events
  ;; events as vector
  [index events]
  (mapv #(riemann-index/insert index %) events))

(defn restore-index
  [index index-backup-edn-file-path]
  (let [backed-up-index-events (try-open-edn index-backup-edn-file-path)]
    (inject-events index backed-up-index-events)))
