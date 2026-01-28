(ns active-riemann.index
  "Functionality related to the Riemann index."
  (:require [clojure.edn :as clojure-edn]
            [active-riemann.logging :as log]
            [riemann.index :as riemann-index]))

;; TODOs
;; - Rename restore-index (we add events to an existing index, it is not a real restore)
;; - add more logs
;; - add proper documentation
;; - add test for new functionality

;; =================== backup index

(defn try-store
  [path-to-file data]
  (try (spit path-to-file (or data []) :append false)
       (log/info `try-store-edn "Successfully stored riemann-index backup file:" path-to-file)
       data
       (catch java.lang.IllegalArgumentException e
         (log/error `try-store-edn "no riemann-index backup file specified:" path-to-file (pr-str e)))
       (catch Exception e
         (log/error `try-store-edn "Error writing riemann-index backup file:" (pr-str e)))))

;; TODO: is there an alternative for (mapv identity ...)?
(defn index->vector
  [index]
  (mapv identity (riemann-index/search index "*")))

(defn backup-index
  [index index-backup-file-path]
  (try-store index-backup-file-path (index->vector index)))

;; =================== add events to index

;; load events

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

;; dummy-functions

(def dummy-event->event identity)

(defn dummy-insertion-approval
  [_index _event]
  true)

;; insertion approval

;; >>> on a running index

(defn- common-query-ast
  [event]
  (str "(host = " (:host event) ") and (service = " (:service event) ") and (time >= " (:time event) ")"))

(defn util-insertion-approval-on-running-index
  [index event]
  (if (and (:host event) (:service event) (:time event))
    (empty? (riemann-index/search index (common-query-ast event)))
    false))

;; <<< on a running index

;; >>> on a copied index

;; index as vector
(defn util-insertion-approval-on-index-copy
  [index event]
  (empty?
   (filter
    #((and (= (:host %) (:host event))
           (= (:service %) (:service event))
           (>= (:time %) (:time event))))
    index)))

;; <<< on a copied index

;; approval runs on event, not on (f-event->event event)
;; TODO; log approval-true/false; log insert? since return value is always `nil`?
(defn- insert
  ([index index-copy event f-insertion-approval f-event->event]
   (if (f-insertion-approval index-copy event)
     (riemann-index/insert index (f-event->event event))
     nil)))

;; running-or-copy: run f-insertion-approval with the current index or a copy?
;;                  used to minimise race-conditions and performance issues
;;                  TODO: add a note on race-conditions and performance issues
;; f-insertion-approval: conditions on which event is inserted into index (note: check is run on event, not (f-event->event)
;; f-event->event: adjust event before inserting
;; returns a vector containing `nil`s or log/error-output
(defn insert-events
  ;; events as vector
  ([index events running-or-copy]
   (insert-events index events running-or-copy dummy-insertion-approval dummy-event->event))

  ([index events running-or-copy f-insertion-approval]
   (insert-events index events running-or-copy f-insertion-approval dummy-event->event))

  ([index events running-or-copy f-insertion-approval f-event->event]

   (case running-or-copy
     :running (mapv #(insert index index % f-insertion-approval f-event->event) events)
     ;; TODO: make sure that (index->vector index) is only called once!
     :copy (let [index-copy (index->vector index)]
             (mapv #(insert index index-copy % f-insertion-approval f-event->event) events))
     _ (log/error `insert-events "missing tag that indicates whether the approval runs on the real index or a copy of the index"))))

;; only use on a fresh index, where no other action runs concurrent on this index!
(defn restore-index
  [index index-backup-edn-file-path]
  (let [backed-up-index-events (try-open-edn index-backup-edn-file-path)]
    (insert-events index backed-up-index-events :copy)))
