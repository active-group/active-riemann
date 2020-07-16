(ns active-riemann.elasticsearch
  "Elasticsearch endpoint."
  (:require [riemann.config :as riemann-config]
            [riemann.elasticsearch :as riemann-elasticsearch]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [clojure.tools.logging :as logging]))

(def hostname
  (let [[_age hostname]
        (riemann.common/get-hostname nil)]
    hostname))

(defn currentTimeSeconds
  []
  (quot (System/currentTimeMillis) 1000))

(defn currentTime
  []
  (clj-time.coerce/from-long (currentTimeSeconds)))

(defn toTime
  [epoch-sec ms]
  (clj-time.coerce/from-long (long (+ (* 1000 epoch-sec) ms))))

(let [iso-fmt (clj-time.format/formatters :date-time)]
  (defn unix-to-iso8602 [epoch-sec ms]
    (clj-time.format/unparse iso-fmt
                             (toTime epoch-sec ms)))
  (defn current-to-iso8602 []
    (clj-time.format/unparse iso-fmt (currentTime))))

(defn bulk-formatter
  "Returns a function which accepts an event and formats it for the Elasticsearch bulk API.
  Options :
  :es-index        Elasticsearch index name (without suffix).
  :es-action       Elasticsearch action, for example \"index\".
  :es-type         Elasticsearch type, for example \"event\", for older Elasticsearch versions. New default is \"_doc\"
  :es-index-suffix Optional index suffix, for example \"-yyyy.MM.dd\".
  Each event received by the function can also have these keys (which override default options), and an optional `es-id` key."
  [{:keys [es-index es-action es-index-suffix]}]
  (fn [event]
    (let [special-keys [:es-index :es-action :es-id :es-type :es-index-suffix :time :time-ms :attributes]
          es-index  (:es-index event es-index)
          es-type   (:es-type event "_doc")
          es-action (:es-action event es-action)
          es-id     (:es-id event)
          es-index-suffix (:es-index-suffix event es-index-suffix)
          ms (if-let [s (:time-ms event)]
               (Integer/parseInt s)
               0)
          timestamp (if-let [tm (:time event)]
                      (unix-to-iso8602 tm ms)
                      (current-to-iso8602))
          source (-> (apply dissoc event special-keys)
                     (assoc (keyword "@timestamp") timestamp))
          metadata (let [m {:_index (str es-index
                                         (when (not-empty es-index-suffix)
                                           (clj-time.format/unparse
                                            (clj-time.format/formatter es-index-suffix)
                                            (if-let [tm (:time event)]
                                              (toTime tm ms)
                                              (currentTime)))))
                            :_type es-type}]
                     (if es-id
                       (assoc m :_id es-id)
                       m))]
      {:es-action es-action
       :es-metadata metadata
       :es-source source})))

(defn elasticsearch-filter
  "Filter the events that should be passed to elasticsearch, anotate them."
  [target annotate-event-fn]
  (riemann-streams/where (and
                          (= "event" (:type event)) ;; only :type "event"
                          (metric nil) ;; no metrics
                          (nil? (:timeout event))) ;; no timeouts
                         (riemann-streams/smap (fn [ev]
                                                 (annotate-event-fn ev))
                                               target)))

(defn make-elasticsearch-stream
  [elasticsearch-url es-index-name & [opts-map]]
  "The stream that passes to elasticsearch."
  (let [{:keys [batch-n batch-dt queue-size core-pool-size max-pool-size es-index-suffix]
         :or {batch-n 100 batch-dt 5
              queue-size 10000 core-pool-size 4 max-pool-size 1024
              es-index-suffix "-yyyy.MM.dd"}} opts-map
        es-bulk (riemann-elasticsearch/elasticsearch-bulk
                 {:es-endpoint elasticsearch-url
                  :formatter (bulk-formatter {:es-index es-index-name
                                              :es-index-suffix es-index-suffix
                                              :es-action "index"})})
        es-bulk-singleton (riemann-test/io (riemann-config/async-queue!
                                            (str ::singleton "-" elasticsearch-url "-" es-index-name)
                                            {:queue-size queue-size :core-pool-size core-pool-size :max-pool-size max-pool-size}
                                            es-bulk))
        es-bulk-batch (riemann-test/io (riemann-config/async-queue!
                                        (str ::batch "-" elasticsearch-url "-" es-index-name)
                                        {:queue-size queue-size :core-pool-size core-pool-size :max-pool-size max-pool-size}
                                        (riemann-streams/batch batch-n batch-dt
                                                               es-bulk)))
        elasticsearch-stream
        (riemann-streams/exception-stream
         (fn [batched-events]
           (logging/warn "Failed to forward" (count (:event batched-events)) "events to Elasticsearch; trying to submit individually")
           (doseq [ev (:event batched-events)]
             ((riemann-streams/exception-stream
               (fn [singleton-event]
                 (logging/warn "Finally failed to forward singleton event to Elasticsearch:" (pr-str (:event singleton-event))))
               es-bulk-singleton) ev)))
         es-bulk-batch)]
    elasticsearch-stream))

(defn make-elasticsearch-typed-stream
  [elasticsearch-url index-base-name type]
  "Annotate index name with type."
  (make-elasticsearch-stream elasticsearch-url (str index-base-name "-" type)))

(defn make-elasticsearch-split-by-type-stream
  [elasticsearch-url index-name]
  (let [event-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "event")
        metric-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "metric")
        state-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "state")]
    (riemann-streams/split*
     #(= "event" (:type %)) event-stream
     #(= "metric" (:type %)) metric-stream
     #(= "state" (:type %)) state-stream
     (riemann-streams/sdo #(logging/debug "type of event not matched in `make-elasticsearch-typed-stream`:" %) event-stream))))
