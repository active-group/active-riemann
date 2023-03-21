(ns active-riemann.elasticsearch
  "Elasticsearch endpoint."
  (:require [riemann.elasticsearch :as riemann-elasticsearch]
            [riemann.streams :as riemann-streams]
            [active-riemann.common :as common]
            [clj-time.format :as time-format]
            [clojure.tools.logging :as logging]))

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
                      (common/unix-to-iso8602 tm ms)
                      (common/current-to-iso8602))
          source (-> (apply dissoc event special-keys)
                     (assoc (keyword "@timestamp") timestamp))
          metadata (let [m {:_index (str es-index
                                         (when (not-empty es-index-suffix)
                                           (time-format/unparse
                                            (time-format/formatter es-index-suffix)
                                            (if-let [tm (:time event)]
                                              (common/toTime tm ms)
                                              (common/currentTime)))))
                            :_type es-type}]
                     (if es-id
                       (assoc m :_id es-id)
                       m))]
      {:es-action es-action
       :es-metadata metadata
       :es-source source})))

(defn elasticsearch-filter
  "Filter the events that should be passed to elasticsearch, anotate them."
  [target pred? annotate-event-fn]
  (riemann-streams/where* pred?
                          (riemann-streams/smap (fn [ev]
                                                  (annotate-event-fn ev))
                                                target)))

(defn make-elasticsearch-stream
  "The stream that passes to elasticsearch."
  [elasticsearch-url es-index-name & [opts-map]]
  (let [{:keys [batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time es-index-suffix]
         :or {batch-n 100 batch-dt 5
              queue-size 10000 core-pool-size 4
              max-pool-size 1024 keep-alive-time 10
              es-index-suffix "-yyyy.MM.dd"}} opts-map
        es-bulk (riemann-elasticsearch/elasticsearch-bulk
                 {:es-endpoint elasticsearch-url
                  :formatter (bulk-formatter {:es-index es-index-name
                                              :es-index-suffix es-index-suffix
                                              :es-action "index"})
                  :http-options {:throw-entire-message? true
                                 :socket-timeout common/timeout-ms}})
        elasticsearch-stream
        (common/batch-with-single-retry (str ::elasticsearch "-" elasticsearch-url "-" es-index-name)
                                        batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time
                                        common/exception-event->ex-data-log-msg
                                        es-bulk)]
    elasticsearch-stream))

(defn make-elasticsearch-typed-stream
  "Annotate index name with type."
  [elasticsearch-url index-base-name type & [opts-map]]
  (make-elasticsearch-stream elasticsearch-url (str index-base-name "-" type) opts-map))

(defn make-elasticsearch-split-by-type-stream
  [elasticsearch-url index-name  & [opts-map]]
  (let [event-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "event" opts-map)
        metric-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "metric" opts-map)
        state-stream (make-elasticsearch-typed-stream elasticsearch-url index-name "state" opts-map)]
    (riemann-streams/split*
     #(= "event" (:type %)) event-stream
     #(= "metric" (:type %)) metric-stream
     #(= "state" (:type %)) state-stream
     (riemann-streams/sdo #(logging/debug "type of event not matched in `make-elasticsearch-typed-stream`:" %) event-stream))))
