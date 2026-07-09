(ns active-riemann.index
  "Functionality related to the Riemann index."
  (:require
   [active-riemann.logging :as log]
   [active.clojure.logger.metric :as metric]
   [active.clojure.logger.timed-metric :as timed-metric]
   [clojure.edn :as clojure-edn]
   [riemann.index :as riemann-index]))

;; util

;; add debug-message and metrics to the riemann-index/search call
(defn- riemann-index-search
  [index query]
  (log/debug `riemann-index-search "calling 'riemann.index/search' with" query)
  (metric/log-counter-metric! "active_riemann_index_search_query_total" 1)
  (timed-metric/log-time-metric!
   #(metric/log-counter-metric! "active_riemann_index_search_query_duration_milliseconds_total" %)
   (riemann-index/search index query)))

(defn index->vector
  [index]
  (timed-metric/log-time-metric!
   #(metric/log-histogram-metric!
     "active_riemann_index_to_vector_duration_milliseconds" [5 10 20 40 80 160] %)
   (mapv identity (riemann-index-search index true))))

;; =================== backup index

(defn normalize-event
  "Takes an entry from the index and makes sure it's a map for safe storage and
  retrieval as edn."
  [index-entry]
  (into {} index-entry))

(defn try-store
  [path-to-file data]
  (try (timed-metric/log-time-metric!
        #(metric/log-histogram-metric!
          "active_riemann_index_write_file_duration_milliseconds" [5 10 20 40 80 160] %)
        (spit path-to-file
              (mapv normalize-event data)
              :append false))
       (log/info `try-store "Successfully stored riemann-index backup file:" path-to-file)
       data
       (catch java.lang.IllegalArgumentException e
         (log/error `try-store "No riemann-index backup file specified:" path-to-file (pr-str e))
         false)
       (catch Exception e
         (log/error `try-store "Error writing riemann-index backup file:" (pr-str e))
         false)))

(defn backup-index
  "Backup a rieman-index in edn-format to a file.
  Arguments:
  - index: the riemann-index
  - index-backup-file-path: a file-path (e.g. `/tmp/riemann-index-backup.edn`)

  Returns:
  - on success: the stored data as vector
  - on failure: false

  For a restore see `insert-events`."
  [index index-backup-file-path]
  (try-store index-backup-file-path (index->vector index)))

;; =================== add events to index

;; load events

(defn try-open-edn
  [path-to-edn-file]
  (try
    (let [edn (timed-metric/log-time-metric!
               #(metric/log-histogram-metric!
                 "active_riemann_index_read_file_duration_milliseconds" [5 10 20 40 80 160] %)
               (clojure-edn/read-string (slurp path-to-edn-file)))]
      (if (or (nil? edn) (empty? edn))
        (do
          (log/info `try-open-edn "Read edn-file:" path-to-edn-file
                    "is nil?" (nil? edn) "or empty?" (empty? edn))
          [])
        (do
          (log/info `try-open-edn
                    "Successfully read edn-file:" path-to-edn-file)
          edn)))
    (catch Exception e
      (do
        (log/error `try-open-edn
                   "Error reading edn-file:" path-to-edn-file
                   "Error message:"
                   (.getMessage e))
        nil))))

;; insert event?

(defn always-insert [_index _event] true)

(defn host-service-query
  "riemann query ast to ask the riemann-index for an event, that has the same
  :host and :service as the provided one."
  [event]
  (list 'and
        (list '= ':host (:host event))
        (list '= ':service (:service event))))

(defn util-insert?
  "true if `event`:
  - contains `:host` and `:service`, and
  - values of host and service are non-`nil`, and
  - this host-service combination does not exist in the riemann-index.
  False otherwise."
  [index event]
  (if (and (:host event) (:service event))
    (empty? (riemann-index-search index (host-service-query event)))
    false))

(defn- insert
  ([index  event insert? f-event->event]
   ;; as documented in `insert-events`:
   ;; insert?-function is called with event, not (f-event->event event)
   (if (insert? index event)
     (try
       (timed-metric/log-time-metric!
        #(metric/log-histogram-metric!
          "active_riemann_index_insert_event_duration_milliseconds" [5 10 20 40 80 160] %)
        (riemann-index/insert index (f-event->event event)))
       (log/info `insert "event inserted into index:" (f-event->event event))
       (metric/log-counter-metric! "active_riemann_index_insert_event_successful_total" 1)
       true
       (catch Exception e
         (log/error `insert "error while inserting the event"
                    (f-event->event event) "into the riemann-index:" (pr-str e))
         (metric/log-counter-metric! "active_riemann_index_insert_event_failed_total" 1)
         false))
     (do
       (log/info `insert "no insertion approval for:" event
                 "event not inserted into index.")
       false))))

(defn insert-events
  "Inserts events to a riemann-index.
  Arguments:
  - index: the riemann-index
  - events: a vector of events to insert
            e.g. [{:host \"localhost\" :service \"foo\" :time 1}]
  - insert? (optional): condition on which an event is inserted into the index
    (`true`) or not (`false`)

    default: `always-insert` (always `true`)
    see `util-insert?` for an alternative

    Keep performance issues in mind - queries to the riemann-index can be
    costly. Race conditions are possible.

    Note: the function gets the 'raw' event, not `(f-event->event event)`
    Note: make sure to provide a proper function, failures are not caught,
          unexpected things could happen

  - f-event->event (optional): function to adjust the event before insertion
    default: `identity`
    Note: make sure to provide a proper function, failures are not caught,
          unexpected things could happen

  Returns:
  - either a boolean-vector indicating insertion (`true`) or skipping (`false`)
    of events
  - or `nil` (with a `active-riemann.logging/error` to indicate the failure)

  Side-effects:
  - insertion of events to the riemann-index
  - log-messages
  - metrics

  Example of usage:
  - Use `backup-index` to create an edn-file with events of your current index.
  - Restart your system and create a fresh riemann-index.
  - Run: `(insert-events index (try-open-edn file-path-to-index-backup-edn))`

  Example of usage:
  - In case of a riemann-index, that already contains events
  - use `util-insert?` as `insert?`
    to make sure that events with existing events with the same 'host' and
    'service' are not overwritten."
  ([index events]
   (insert-events index events always-insert identity))

  ([index events insert?]
   (insert-events index events insert? identity))

  ([index events insert? f-event->event]
   (mapv #(insert index % insert? f-event->event) events)))
