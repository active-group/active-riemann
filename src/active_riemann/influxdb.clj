(ns ^:no-doc active-riemann.influxdb
  "InfluxDB endpoint."
  (:require [riemann.influxdb :as riemann-influxdb]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]
            [active-riemann.common :as common]
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

(defn make-influxdb-stream
  [influxdb-host & [db-name tag-fields opts-map]]
  (let [{:keys [batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time]
         :or {batch-n 1000 batch-dt 1
              queue-size 10000 core-pool-size 1
              max-pool-size 128 keep-alive-time 60000}} opts-map
        db-name (or db-name "riemann")
        influxdb-future
        (future (make-influxdb-connection (merge
                                           {:version :0.9
                                            :host influxdb-host
                                            :db db-name
                                            :timeout 20000}
                                           (if tag-fields {:tag-fields tag-fields} {}))))
        influxdb
        (fn [e]
          (if (realized? influxdb-future)
            (@influxdb-future e)
            (discard-events e)))
        influxdb-stream
        (common/batch-with-single-retry (str ::influxdb "-" influxdb-host "-" db-name)
                                        batch-n batch-dt queue-size core-pool-size max-pool-size keep-alive-time
                                        (fn [exception-event] (str (:exception exception-event)))
                                        influxdb)]
    (riemann-streams/smap #(dissoc % :ttl)
                          (riemann-test/tap ::influxdb
                                            influxdb-stream))))
