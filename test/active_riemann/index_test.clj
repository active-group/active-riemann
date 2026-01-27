(ns active-riemann.index-test
  (:require  [clojure.test :as t]
             [clojure.edn :as clojure-edn]
             [riemann.config :as riemann-config]
             [riemann.index :as riemann-index]
             [riemann.test :as riemann-test]
             [active.clojure.logger.internal :as internal]
             [active-riemann.index :as active-index]
             [test-with-files.tools :refer [with-tmp-dir]]
             [clojure.string :as string]))

(def ^:dynamic *captured-events*
  "A vector of captures events." (atom []))

(defn captured-events []
  @*captured-events*)

(defn cleanup-message [message]
  (cond
    (string/includes?
     (first message)
     "active-riemann.index/try-store-edn no riemann-index backup file specified: nil")
    ["active-riemann.index/try-store-edn no riemann-index backup file specified: nil"]
    (string/includes?
     (first message)
     "active-riemann.index/try-store-edn Successfully stored riemann-index backup file:")
    ["active-riemann.index/try-store-edn Successfully stored riemann-index backup file:"]
    :else message))

(defn capture-event
  [_type _origin level _context message]
  (swap! *captured-events* conj {:level level :message (cleanup-message @message)})
  nil)

(defn each [f]
  (reset! *captured-events* [])
  (with-redefs [internal/log-event!-internal capture-event]
    (f)))

(t/use-fixtures :each each)

(defn file-path [file-name]
  (str "test/resources/" file-name))

(riemann-test/with-test-env
  (t/deftest t-restore-index
    (let [the-index (riemann-config/index)]

      (t/is (= 0 (count (riemann.index/search the-index "*"))))
      (t/is (= [] (captured-events)))

      (active-index/restore-index the-index "not-existing.edn")

      (t/is (= 0 (count (riemann.index/search the-index "*"))))
      (t/is (= [{:level :error,
                 :message
                 ["active-riemann.index/try-open-edn riemann-index backup file not found: not-existing.edn"]}]
               (captured-events)))
      (reset! *captured-events* [])
      (t/is (= [] (captured-events)))

      (active-index/restore-index the-index (file-path "empty.edn"))

      (t/is (= 0 (count (riemann.index/search the-index "*"))))
      (t/is (= [] (captured-events)))

      (active-index/restore-index the-index (file-path "invalid-content.edn"))

      (t/is (= 0 (count (riemann.index/search the-index "*"))))
      (t/is (= [{:level :error,
                 :message
                 ["active-riemann.index/try-open-edn Error reading riemann-index backup file: test/resources/invalid-content.edn Invalid number: +1j"]}]
               (captured-events)))
      (reset! *captured-events* [])
      (t/is (= [] (captured-events)))

      (riemann-index/insert the-index {:host "the-host" :service "baz" :time 124})
      (active-index/restore-index the-index (file-path "riemann-index-backup.edn"))
      (riemann-index/insert the-index {:host "the-host" :service "biz" :time 125})

      (t/is (= 4 (count (riemann.index/search the-index "*"))))
        ;; TODO: is there a logical order in this result?
      (t/is (= [{:host "the-host" :service "biz" :time 125}
                {:host "localhost" :service "foo" :metric 10, :time 123}
                {:host "the-host" :service "baz" :time 124}
                {:host "localhost" :service "bar" :metric 10, :time 123}]
               (riemann.index/search the-index "*")))
      (t/is (= [] (captured-events))))))

(riemann-test/with-test-env
  (t/deftest t-backup-index
    (with-tmp-dir tmp-dir
      (let [the-index (riemann-config/index)]
        (mapv #(riemann-index/insert the-index %)
              [{:host "the-host" :service "biz" :time 125}
               {:host "localhost", :service "foo", :metric 10, :time 123}
               {:host "the-host" :service "baz" :time 124}
               {:host "localhost", :service "bar", :metric 10, :time 123}])

        (t/is (= 4 (count (riemann.index/search the-index "*"))))
        (t/is (= [] (captured-events)))

        (active-index/backup-index the-index nil)
        (t/is (= [{:level :error,
                   :message
                   ["active-riemann.index/try-store-edn no riemann-index backup file specified: nil"]}]
                 (captured-events)))
        (reset! *captured-events* [])
        (t/is (= [] (captured-events)))

          ;; valid-file-path: file does not exist: creates file && file contains the four metrics
        (active-index/backup-index the-index (str tmp-dir "riemann-index-backup.edn"))
        (t/is (= [{:level :info,
                   :message
                   ["active-riemann.index/try-store-edn Successfully stored riemann-index backup file:"]}]
                 (captured-events)))
        (t/is (= [{:host "the-host" :service "biz" :time 125}
                  {:host "localhost", :service "foo", :metric 10, :time 123}
                  {:host "the-host" :service "baz" :time 124}
                  {:host "localhost", :service "bar", :metric 10, :time 123}]
                 (clojure-edn/read-string (slurp (str tmp-dir "riemann-index-backup.edn")))))
        (reset! *captured-events* [])
        (t/is (= [] (captured-events)))

          ;; valid-file-path: file does exist: overwrite file && file contains the four metrics
        (spit (str tmp-dir "riemann-index-backup.edn") ["hello" "world"] :append false)
        (t/is (= ["hello" "world"]
                 (clojure-edn/read-string (slurp (str tmp-dir "riemann-index-backup.edn")))))
        (active-index/backup-index the-index (str tmp-dir "riemann-index-backup.edn"))
        (t/is (= [{:level :info,
                   :message
                   ["active-riemann.index/try-store-edn Successfully stored riemann-index backup file:"]}]
                 (captured-events)))
        (t/is (= [{:host "the-host" :service "biz" :time 125}
                  {:host "localhost", :service "foo", :metric 10, :time 123}
                  {:host "the-host" :service "baz" :time 124}
                  {:host "localhost", :service "bar", :metric 10, :time 123}]
                 (clojure-edn/read-string (slurp (str tmp-dir "riemann-index-backup.edn")))))))))
