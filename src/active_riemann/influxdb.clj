(ns ^:no-doc active-riemann.influxdb
  "InfluxDB endpoint."
  (:require [riemann.config :as riemann-config]
            [riemann.influxdb :as riemann-influxdb]
            [riemann.streams :as riemann-streams]
            [riemann.test :as riemann-test]))

(defn make-influxdb-stream
  [influxdb-host & [db-name tag-fields]]
  (riemann-streams/smap #(dissoc % :ttl)
                        (riemann-test/io
                         (riemann-streams/batch 100 1/10
                                                (riemann-config/async-queue! (str ::influx "-" influxdb-host "-" db-name)
                                                                             {:queue-size 10000
                                                                              :core-pool-size 1
                                                                              :max-pool-size 128
                                                                              :keep-alive-time 60000}
                                                                             (riemann-influxdb/influxdb (merge
                                                                                                         {:version :0.9
                                                                                                          :host influxdb-host}
                                                                                                         (if db-name {:db db-name} {})
                                                                                                         (if tag-fields {:tag-fields tag-fields} {}))))))))
