(ns active-riemann.index
  "Functionality related to the Riemann index."
  (:require [clojure.edn :as clojure-edn]
            [active-riemann.logging :as log]
            [riemann.index :as riemann-index]))

(defn- try-open
  [path-to-edn-file]
  (try (let [edn (clojure-edn/read-string (slurp path-to-edn-file))]
         (if (or (nil? edn) (empty? edn))
           []
           edn))
       (catch java.io.FileNotFoundException _e
         (log/error `try-open-edn "riemann-index backup file not found:" path-to-edn-file))
       (catch Exception e
         (log/error `try-open-edn "Error reading riemann-index backup file:" path-to-edn-file (.getMessage e)))))

(defn- try-store
  [path-to-edn-file data]
  (try (spit path-to-edn-file (or data []) :append false)
       (log/info `try-store-edn "Successfully stored riemann-index backup file:" path-to-edn-file)
       data
       (catch java.lang.IllegalArgumentException e
         (log/error `try-store-edn "no riemann-index backup file specified:" path-to-edn-file (pr-str e)))
       (catch Exception e
         (log/error `try-store-edn "Error writing riemann-index backup file:" (pr-str e)))))

(defn restore-index
  [index index-backup-edn-file-path]
  (let [backed-up-index-events (try-open index-backup-edn-file-path)]
    (mapv #(riemann-index/insert index %) backed-up-index-events)))

(defn backup-index
  [index index-backup-edn-file-path]
  (try-store index-backup-edn-file-path (mapv identity (riemann-index/search index "*"))))
