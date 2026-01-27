(ns active-riemann.index-test
  (:require  [clojure.test :as t]
             [clojure.edn :as clojure-edn]
             [riemann.config :as riemann-config]
             [riemann.index :as riemann-index]
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

(t/deftest t-restore-index-no-edn-file
  (active-index/restore-index (riemann-config/index) "not-existing.edn")
  (t/is (= [{:level :error,
             :message
             ["active-riemann.index/try-open-edn riemann-index backup file not found: not-existing.edn"]}]
           (captured-events))))

;; restore index

(t/deftest t-restore-index-empty-edn-file
  (let [the-index (riemann-config/index)]
    (t/is (= [] (riemann-index/search the-index "*")))
    (t/is (= [] (captured-events)))

    (active-index/restore-index the-index (file-path "empty.edn"))

    ;; nothing added to the index
    (t/is (= [] (riemann-index/search the-index "*")))
    (t/is (= [] (captured-events)))

    (riemann-index/insert the-index {:host "the-host" :service "baz" :time 124})
    (t/is (= [{:host "the-host" :service "baz" :time 124}]
             (riemann-index/search the-index "*")))

    ;; nothing added to the index and nothing existing overwritten in the index
    (active-index/restore-index the-index (file-path "empty.edn"))

    (t/is (= [{:host "the-host" :service "baz" :time 124}]
             (riemann-index/search the-index "*")))
    (t/is (= [] (captured-events)))))

(t/deftest t-restore-index-invalid-edn-file
  (let [the-index (riemann-config/index)]
    (active-index/restore-index the-index (file-path "invalid-content.edn"))

    ;; nothing added to the index
    (t/is (= [] (riemann-index/search the-index "*")))
    (t/is (= [{:level :error,
               :message
               ["active-riemann.index/try-open-edn Error reading riemann-index backup file: test/resources/invalid-content.edn Invalid number: +1j"]}]
             (captured-events)))
    (reset! *captured-events* [])
    (t/is (= [] (captured-events)))

    (riemann-index/insert the-index {:host "the-host" :service "baz" :time 124})
    (t/is (= [{:host "the-host" :service "baz" :time 124}]
             (riemann-index/search the-index "*")))

    ;; nothing added to the index and nothing existing overwritten in the index
    (active-index/restore-index the-index (file-path "invalid-content.edn"))

    (t/is (= [{:host "the-host" :service "baz" :time 124}]
             (riemann-index/search the-index "*")))
    (t/is (= [{:level :error,
               :message
               ["active-riemann.index/try-open-edn Error reading riemann-index backup file: test/resources/invalid-content.edn Invalid number: +1j"]}]
             (captured-events)))))

(t/deftest t-restore-index-edn-file-with-content-no-index-overlap
  (let [the-index (riemann-config/index)]
    (riemann-index/insert the-index {:host "the-host" :service "baz" :time 124})
    (active-index/restore-index the-index (file-path "riemann-index-backup.edn"))
    (riemann-index/insert the-index {:host "the-host" :service "biz" :time 125})

    (t/is (= [{:host "localhost" :service "bar" :metric 10 :time 123}
              {:host "the-host" :service "baz" :time 124}
              {:host "the-host" :service "biz" :time 125}
              {:host "localhost" :service "foo" :metric 10 :time 123}]
             (sort-by :service (riemann-index/search the-index "*"))))
    (t/is (= [] (captured-events)))))

;; FIXME: do we want to overwrite a newer event in the new index?
(t/deftest t-restore-index-edn-file-with-content-with-index-overlap
  (let [the-index (riemann-config/index)]
    (riemann-index/insert the-index {:host "localhost" :service "foo" :metric 10 :time 124})
    (active-index/restore-index the-index (file-path "riemann-index-backup.edn"))

    ;; note: using sort (not set) to see whether the index contains duplicates
    (t/is (= [{:host "localhost" :service "bar" :metric 10 :time 123}
              {:host "localhost" :service "foo" :metric 10 :time 123}]
             (sort-by :service (riemann-index/search the-index "*"))))
    (t/is (= [] (captured-events)))))

;; backup-index

(t/deftest t-backup-index-no-edn-file-path
  (let [the-index (riemann-config/index)]
    (mapv #(riemann-index/insert the-index %)
          [{:host "the-host" :service "biz" :time 125}
           {:host "localhost" :service "foo" :metric 10 :time 123}
           {:host "the-host" :service "baz" :time 124}
           {:host "localhost" :service "bar" :metric 10 :time 123}])

    (t/is (= [{:host "the-host" :service "biz" :time 125}
              {:host "localhost" :service "foo" :metric 10 :time 123}
              {:host "the-host" :service "baz" :time 124}
              {:host "localhost" :service "bar" :metric 10 :time 123}]
             (riemann-index/search the-index "*")))

    (active-index/backup-index the-index nil)
    (t/is (= [{:level :error,
               :message
               ["active-riemann.index/try-store-edn no riemann-index backup file specified: nil"]}]
             (captured-events)))))

;; valid-file-path: file does not exist: creates file && file contains the four metrics
(t/deftest t-backup-index-to-new-file
  (with-tmp-dir tmp-dir
    (let [the-index (riemann-config/index)
          file-path (str tmp-dir "riemann-index-backup.edn")]
      (mapv #(riemann-index/insert the-index %)
            [{:host "the-host" :service "biz" :time 125}
             {:host "localhost" :service "foo" :metric 10 :time 123}
             {:host "the-host" :service "baz" :time 124}
             {:host "localhost" :service "bar" :metric 10 :time 123}])

      (active-index/backup-index the-index file-path)

      (t/is (= [{:level :info,
                 :message
                 ["active-riemann.index/try-store-edn Successfully stored riemann-index backup file:"]}]
               (captured-events)))
      (t/is (= [{:host "the-host" :service "biz" :time 125}
                {:host "localhost", :service "foo", :metric 10, :time 123}
                {:host "the-host" :service "baz" :time 124}
                {:host "localhost", :service "bar", :metric 10, :time 123}]
               (clojure-edn/read-string (slurp file-path)))))))

;; valid-file-path: file does exist: overwrite file && file contains the four metrics
(t/deftest t-backup-index-to-existing-file
  (with-tmp-dir tmp-dir
    (let [the-index (riemann-config/index)
          file-path (str tmp-dir "riemann-index-backup.edn")]
      (mapv #(riemann-index/insert the-index %)
            [{:host "the-host" :service "biz" :time 125}
             {:host "localhost" :service "foo" :metric 10 :time 123}
             {:host "the-host" :service "baz" :time 124}
             {:host "localhost" :service "bar" :metric 10 :time 123}])

      (spit file-path ["hello" "world"])
      (t/is (= ["hello" "world"]
               (clojure-edn/read-string (slurp file-path))))

      (active-index/backup-index the-index file-path)

      (t/is (= [{:level :info,
                 :message
                 ["active-riemann.index/try-store-edn Successfully stored riemann-index backup file:"]}]
               (captured-events)))
      (t/is (= [{:host "the-host" :service "biz" :time 125}
                {:host "localhost", :service "foo", :metric 10, :time 123}
                {:host "the-host" :service "baz" :time 124}
                {:host "localhost", :service "bar", :metric 10, :time 123}]
               (clojure-edn/read-string (slurp file-path)))))))
