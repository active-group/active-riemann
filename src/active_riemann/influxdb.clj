(ns ^:no-doc active-riemann.influxdb
  "InfluxDB endpoint."
  (:require [riemann.config :as riemann-config]
            [riemann.influxdb :as riemann-influxdb]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [clojure.tools.logging :as logging]))

(defn make-influxdb-connection
  [opts]
  (try (let [influxdb (riemann-influxdb/influxdb opts)]
         (logging/info "Connected to influxdb " opts)
         influxdb)
       (catch Exception e
         (logging/warn "Caught event when creating influxdb connection " opts ":" (.getMessage e) ", trying again in 3s.")
         (Thread/sleep 3000)
         (make-influxdb-connection opts))))

(defn discard-events
  [e]
  (logging/debug "Connection to influxdb not ready, discarding " (count (:event e)) " events"))

(def default-db-name "riemann")

(defn make-influxdb-stream
  [influxdb-host & [db-name tag-fields opts-map]]
  (let [{:keys [batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time]
         :or {batch-n 1000 batch-dt 1
              queue-size 10000 core-pool-size 1
              max-pool-size 128 keep-alive-time 60000}} opts-map
        influxdb-future
        (future (make-influxdb-connection (merge
                                           {:version :0.9
                                            :host influxdb-host}
                                           (if db-name {:db db-name} {:db default-db-name})
                                           (if tag-fields {:tag-fields tag-fields} {}))))
        influxdb
        (fn [e]
          (if (realized? influxdb-future)
            (@influxdb-future e)
            (discard-events e)))
        influxdb-singleton
        (riemann-test/io
         (riemann-config/async-queue! (str ::influx-singleton "-" influxdb-host "-" (or db-name default-db-name))
                                      {:queue-size queue-size
                                       :core-pool-size core-pool-size
                                       :max-pool-size max-pool-size
                                       :keep-alive-time keep-alive-time}
                                      influxdb))
        influxdb-batch
        (riemann-test/io
         (riemann-streams/batch batch-n batch-dt
                                (riemann-config/async-queue! (str ::influx-batch "-" influxdb-host "-" (or db-name default-db-name))
                                                             {:queue-size queue-size
                                                              :core-pool-size core-pool-size
                                                              :max-pool-size max-pool-size
                                                              :keep-alive-time keep-alive-time}
                                                             influxdb)))
        influxdb-stream
        (riemann-streams/exception-stream
         (fn [batched-events]
           (logging/warn "Failed to forward" (count (:event batched-events)) "events to influxdb; trying to submit individually")
           (logging/debug (let [exd (ex-data (:exception batched-events))] (str (:status exd) " " (pr-str (:body exd)))))
           (doseq [ev (:event batched-events)]
             ((riemann-streams/exception-stream
               (fn [singleton-event]
                 (logging/warn "Finally failed to forward singleton event to influxdb:" (pr-str (:event singleton-event)))
                 (logging/debug (let [exd (ex-data (:exception singleton-event))] (str (:status exd) " " (pr-str (:body exd))))))
               influxdb-singleton) ev)))
         influxdb-batch)]
    (riemann-streams/smap #(dissoc % :ttl)
                          (riemann-test/tap :influxdb
                                            influxdb-stream))))
