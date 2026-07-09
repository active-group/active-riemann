(ns active-riemann.index-test
  (:require
   [active-riemann.index :as active-index]
   [active.clojure.logger.internal :as internal]
   [clojure.edn :as clojure-edn]
   [clojure.string :as string]
   [clojure.test :as t]
   [clojure.test.check.clojure-test :as ct]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [riemann.codec :as codec]
   [riemann.config :as riemann-config]
   [riemann.index :as riemann-index]
   [riemann.query :as riemann-query]
   [test-with-files.tools :refer [with-tmp-dir]]))

;; =================== setup

(def ^:dynamic *captured-events*
  "A vector of captures events." (atom []))

(defn captured-events []
  @*captured-events*)

(defn cleanup-message [message]
  (cond
    (string/includes?
     (first message)
     "active-riemann.index/try-store No riemann-index backup file specified: nil")
    ["active-riemann.index/try-store No riemann-index backup file specified: nil"]
    :else message))

(defn capture-event
  [type _origin level _context message]
  (case type
    "metric" nil ;; exclude metrics
    (swap! *captured-events* conj {:level level :message (cleanup-message @message)}))
  nil)

(defn each [f]
  (reset! *captured-events* [])
  (with-redefs [internal/log-event!-internal capture-event]
    (f)))

(t/use-fixtures :each each)

(defn file-path [file-name]
  (str "test/resources/" file-name))

(defn- get-index-content
  [index]
  (riemann-index/search index true))

;; =================== backup index

;; no-file-path
(t/deftest t-backup-index-no-file-path
  ;; prepare
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
             (get-index-content the-index)))

    ;; call
    (t/is (= false (active-index/backup-index the-index nil)))

    ;; result
    (t/is (= [{:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with true")]}
              {:level :error,
               :message
               [(str "active-riemann.index/try-store "
                     "No riemann-index backup file specified: nil")]}]
             (captured-events)))))

;; valid-file-path: file does not exist:
;;   creates file && file contains the four metrics
(t/deftest t-backup-index-to-new-file
  ;; prepare
  (with-tmp-dir tmp-dir
    (let [the-index (riemann-config/index)
          the-file-path (str tmp-dir "riemann-index-backup.edn")
          data  [{:host "the-host" :service "biz" :time 125}
                 {:host "localhost" :service "foo" :metric 10 :time 123}
                 {:host "the-host" :service "baz" :time 124}
                 {:host "localhost" :service "bar" :metric 10 :time 123}]]

      (mapv #(riemann-index/insert the-index %) data)

      ;; call
      (t/is (= data (active-index/backup-index the-index the-file-path)))

      ;; result
      (t/is (= [{:level :debug,
                 :message
                 [(str "active-riemann.index/riemann-index-search calling "
                       "'riemann.index/search' with true")]}
                {:level :info,
                 :message
                 [(str "active-riemann.index/try-store "
                       "Successfully stored riemann-index backup file: "
                       the-file-path)]}]
               (captured-events)))
      (t/is (= data (clojure-edn/read-string (slurp the-file-path)))))))

;; valid-file-path: file does exist:
;;   overwrite file && file contains the four metrics
(t/deftest t-backup-index-to-existing-file
  ;; prepare
  (with-tmp-dir tmp-dir
    (let [the-index (riemann-config/index)
          the-file-path (str tmp-dir "riemann-index-backup.edn")
          data [{:host "the-host" :service "biz" :time 125}
                {:host "localhost" :service "foo" :metric 10 :time 123}
                {:host "the-host" :service "baz" :time 124}
                {:host "localhost" :service "bar" :metric 10 :time 123}]]

      (mapv #(riemann-index/insert the-index %) data)

      (spit the-file-path ["hello" "world"])
      (t/is (= ["hello" "world"]
               (clojure-edn/read-string (slurp the-file-path))))

      ;; call
      (t/is (= data (active-index/backup-index the-index the-file-path)))

      ;; result
      (t/is (= [{:level :debug,
                 :message
                 [(str "active-riemann.index/riemann-index-search calling "
                       "'riemann.index/search' with true")]}
                {:level :info,
                 :message
                 [(str "active-riemann.index/try-store "
                       "Successfully stored riemann-index backup file: "
                       the-file-path)]}]
               (captured-events)))
      (t/is (= data (clojure-edn/read-string (slurp the-file-path)))))))

;; =================== add events to index

(t/deftest t-try-open-edn-file-path-is-nil
  ;; prepare
  (let [the-file-path nil]

    ;; call
    (t/is (= nil (active-index/try-open-edn the-file-path)))

    ;; result
    (t/is (= [{:level :error,
               :message
               [(str "active-riemann.index/try-open-edn "
                     "Error reading edn-file: nil "
                     "Error message: "
                     "Cannot open <nil> as a Reader.")]}]
             (captured-events)))))

(t/deftest t-try-open-edn-file-does-not-exist
  ;; prepare
  (let [the-file-path "not-existing.edn"]

    ;; call
    (t/is (= nil (active-index/try-open-edn the-file-path)))

    ;; result
    (t/is (= [{:level :error,
               :message
               [(str "active-riemann.index/try-open-edn "
                     "Error reading edn-file: " the-file-path
                     " Error message: " the-file-path
                     " (No such file or directory)")]}]
             (captured-events)))))

(t/deftest t-try-open-edn-file-is-empty
  ;; prepare
  (let [the-file-path (file-path "empty.edn")]

    ;; call
    (t/is (= [] (active-index/try-open-edn the-file-path)))

    ;; result
    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/try-open-edn "
                     "Read edn-file: " the-file-path " is nil? true or empty? true")]}]
             (captured-events)))))

(t/deftest t-try-open-edn-file-invalid-content
  ;; prepare
  (let [the-file-path (file-path "invalid-content.edn")]

    ;; call
    (t/is (= nil (active-index/try-open-edn the-file-path)))
    ;; result
    (t/is (= [{:level :error,
               :message
               [(str "active-riemann.index/try-open-edn "
                     "Error reading edn-file: " the-file-path
                     " Error message: "
                     "Invalid number: +1j")]}]
             (captured-events)))))

(t/deftest t-try-open-edn-file-valid-content
  ;; prepare
  (let [the-file-path (file-path "riemann-index-backup.edn")]

    ;; call
    (t/is (= [{:host "localhost", :service "foo", :metric 10, :time 1}
              {:host "localhost", :service "bar", :metric 10, :time 1}]
             (active-index/try-open-edn the-file-path)))

    ;; result
    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/try-open-edn "
                     "Successfully read edn-file: " the-file-path)]}]
             (captured-events)))))

(t/deftest host-service-query-test
  (let [event-1 {:host "localhost" :service "foo" :metric 10 :time 123}
        event-2 {:host 1 :service 1 :metric 10 :time 123}]

    (t/is (= (riemann-query/ast "(host = \"localhost\") and (service = \"foo\")")
             (active-index/host-service-query event-1)))

    (t/is (= (riemann-query/ast "(host = 1) and (service = 1)")
             (active-index/host-service-query event-2)))))

(t/deftest t-util-insert?
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost" :service "foo" :metric 10 :time 123}
        event-2 {:host "localhost" :service "bar" :metric 10 :time 123}
        event-3 {:host 1 :service 1 :metric 10 :time 123}]

    ;; event-1 not in index
    (t/is (= true (active-index/util-insert? the-index event-1)))

    (riemann-index/insert the-index event-1)

    ;; event-1 is in index
    (t/is (= false (active-index/util-insert? the-index event-1)))

    ;; event-2 is not in index
    (t/is (= true (active-index/util-insert? the-index event-2)))

    ;; event-3 is not in index
    (t/is (= true (active-index/util-insert? the-index event-3)))

    (riemann-index/insert the-index event-3)

    ;; event-3 is in index
    (t/is (= false (active-index/util-insert? the-index event-3)))))

;; restore index

;; note: be aware of the side effects of `active-index/insert-events`

(t/deftest t-insert-events-edge-cases-on-empty-index
  (let [the-index (riemann-config/index)
        nil-events nil
        empty-events []
        event-1 {:host "localhost" :service "foo" :time 1}
        event-2 {:host "localhost" :service "bar" :time 1}
        broken-event {:host "localhost" :service "baz"}
        broken-events [event-1, broken-event, event-2]]

    (t/is (= [] (active-index/insert-events the-index nil-events)))
    (t/is (= [] (get-index-content the-index)))
    (t/is (= [] (captured-events)))

    (t/is (= [] (active-index/insert-events the-index empty-events)))
    (t/is (= [] (get-index-content the-index)))
    (t/is (= [] (captured-events)))

    (t/is (= [true, false, true] (active-index/insert-events the-index broken-events)))
    (t/is (= [event-1, event-2] (get-index-content the-index)))
    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"foo\", :time 1}")]}
              {:level :error,
               :message
               [(str "active-riemann.index/insert error while inserting the event "
                     "{:host \"localhost\", :service \"baz\"} into the riemann-index: "
                     "clojure.lang.ExceptionInfo: "
                     "cannot index event with no time "
                     "{:event {:host \"localhost\", :service \"baz\"}}")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"bar\", :time 1}")]}]
             (captured-events)))))

(t/deftest t-insert-events-no-events-on-non-empty-index
  (let [the-index (riemann-config/index)
        event {:host "the-host" :service "baz" :time 124}]

    (riemann-index/insert the-index event)
    (t/is (= [event] (get-index-content the-index)))

    ;; nothing added to the index and nothing existing overwritten in the index
    ;; events: nil
    (t/is (= [] (active-index/insert-events the-index nil)))
    (t/is (= [event] (get-index-content the-index)))
    (t/is (= [] (captured-events)))

    ;; nothing added to the index and nothing existing overwritten in the index
    ;; events: []
    (t/is (= [] (active-index/insert-events the-index [])))
    (t/is (= [event] (get-index-content the-index)))
    (t/is (= [] (captured-events)))))

;; events in index do not overlap with events that are inserted
(t/deftest t-insert-events-no-event-overlap
  (let [the-index (riemann-config/index)
        event-0 {:host "the-host" :service "baz" :time 4}
        event-1 {:host "localhost", :service "foo", :metric 10, :time 1}
        event-2 {:host "localhost", :service "bar", :metric 10, :time 1}
        event-3 {:host "the-host" :service "biz" :time 5}
        backed-up-events [event-1 event-2]]

    (riemann-index/insert the-index event-0)

    (t/is (= [true true] (active-index/insert-events the-index backed-up-events)))

    (riemann-index/insert the-index event-3)

    ;; note: using sort (not set) to see whether the index contains duplicates
    (t/is (= [event-2 event-0 event-3 event-1] ; bar baz biz foo
             (sort-by :service (get-index-content the-index))))
    (t/is (= [{:level :info
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"foo\", :metric 10, :time 1}")]}
              {:level :info
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"bar\", :metric 10, :time 1}")]}]
             (captured-events)))))

;; events in the index are overwritten by inserted events
(t/deftest t-insert-events-event-index-overlap-overwrite
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost" :service "foo" :time 1}
        event-2 {:host "localhost" :service "foo" :time 2}]

    (riemann-index/insert the-index event-1)

    (t/is (= [event-1] (get-index-content the-index)))
    (t/is (= [true] (active-index/insert-events the-index [event-1])))
    (t/is (= [event-1] (get-index-content the-index)))

    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"foo\", :time 1}")]}]
             (captured-events)))
    (reset! *captured-events* [])

    (t/is (= [true] (active-index/insert-events the-index [event-2])))
    (t/is (= [event-2] (get-index-content the-index)))
    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"foo\", :time 2}")]}]
             (captured-events)))))

;; events in index do overlap with events that are inserted
;; insert? checks for `:host` and `:service` (`util-insert?`)
(t/deftest t-insert-events-events-index-overlap-conditionally-overwrite
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost", :service "foo", :metric 10, :time 1}
        event-2  {:host "localhost", :service "bar", :metric 10, :time 1}
        backed-up-events [event-1 event-2]
        event-1-1 (assoc event-1 :metric 0 :time 2)]

    (riemann-index/insert the-index event-1-1)

    (t/is (= [false true]
             (active-index/insert-events the-index backed-up-events active-index/util-insert?)))

    ;; note: using sort (not set) to see whether the index contains duplicates
    (t/is (= [event-2 event-1-1] (sort-by :service (get-index-content the-index))))
    (t/is (= [{:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"foo\"))")]}
              {:level :info
               :message
               [(str "active-riemann.index/insert no insertion approval for: "
                     "{:host \"localhost\", :service \"foo\", :metric 10, :time 1} "
                     "event not inserted into index.")]}
              {:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"bar\"))")]}
              {:level :info
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"bar\", :metric 10, :time 1}")]}]
             (captured-events)))))

(t/deftest t-insert-events-util-insert?
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost" :service "foo" :time 1}
        event-2 {:host "localhost" :service "bar" :time 2}
        event-3 {:host "localhost" :service "biz" :time 2}
        event-1-1 (assoc event-1 :time 3)]

    (riemann-index/insert the-index event-1-1)

    (t/is (= [false, false, false, true, true]
             (active-index/insert-events
              the-index
              [event-1, (assoc event-1 :time 5), (assoc event-1 :time 4), event-2, event-3]
              active-index/util-insert?)))

    ;; event-1 (foo) not replaced
    (t/is (= [event-2 event-3 event-1-1] (sort-by :service (get-index-content the-index))))

    (t/is (= [{:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"foo\"))")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert no insertion approval for: "
                     "{:host \"localhost\", :service \"foo\", :time 1} "
                     "event not inserted into index.")]}
              {:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"foo\"))")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert no insertion approval for: "
                     "{:host \"localhost\", :service \"foo\", :time 5} "
                     "event not inserted into index.")]}
              {:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"foo\"))")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert no insertion approval for: "
                     "{:host \"localhost\", :service \"foo\", :time 4} "
                     "event not inserted into index.")]}
              {:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"bar\"))")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"bar\", :time 2}")]}
              {:level :debug,
               :message
               [(str "active-riemann.index/riemann-index-search calling "
                     "'riemann.index/search' with "
                     "(and (= :host \"localhost\") (= :service \"biz\"))")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"biz\", :time 2}")]}]
             (captured-events)))))

;; using (f-events->events event)
(t/deftest t-insert-events-adjust-events
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost" :service "foo" :time 1}
        event-2 {:host "localhost" :service "bar" :time 1}
        event-3 {:host "localhost" :service "biz" :time 1}
        f-event->event #(case (:service %)
                          "foo" (assoc % :restored true)
                          "bar" (assoc % :ttl 100)
                          %)]

    (riemann-index/insert the-index event-1)

    (t/is (= [event-1] (get-index-content the-index)))
    (t/is (= [true, true, true]
             (active-index/insert-events the-index
                                         [event-1, event-2, event-3]
                                         active-index/always-insert
                                         f-event->event)))
    (t/is (= [{:host "localhost" :service "bar" :ttl 100 :time 1}
              {:host "localhost" :service "biz" :time 1}
              {:host "localhost" :service "foo" :restored true :time 1}]
             (sort-by :service (get-index-content the-index))))

    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"foo\", :time 1, :restored true}")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"bar\", :time 1, :ttl 100}")]}
              {:level :info,
               :message
               [(str "active-riemann.index/insert event inserted into index: "
                     "{:host \"localhost\", :service \"biz\", :time 1}")]}]
             (captured-events)))))

;; insert? runs on 'raw' event not on (f-event->event event)
(t/deftest t-insert-events-insert?-on-raw-event
  (let [the-index (riemann-config/index)
        event-1 {:host "localhost" :service "foo" :time 1}
        f-insert? (fn [_index event]
                    (if (and (contains? event :restore)
                             (boolean? (:restore event)))
                      (:restore event)
                      false))
        f-event->event #(assoc % :restore true)]

    (t/is (= [false]
             (active-index/insert-events the-index [event-1]
                                         f-insert?
                                         f-event->event)))

    (t/is (= [] (get-index-content the-index)))

    (t/is (= [{:level :info,
               :message
               [(str "active-riemann.index/insert no insertion approval for: "
                     "{:host \"localhost\", :service \"foo\", :time 1} "
                     "event not inserted into index.")]}]
             (captured-events)))))

(def riemann-event-gen
  "Generator for events as Riemann might emit them."
  (let [state-gen (gen/elements ["A" "B" "C"])
        nilable-string-gen (gen/one-of [(gen/return nil) (gen/not-empty gen/string-alphanumeric)])
        metric-gen (gen/one-of [(gen/return nil)
                                gen/large-integer
                                (gen/double* {:infinite? false :NaN? false})])
        tags-gen (gen/one-of [(gen/return nil)
                              (gen/vector (gen/not-empty gen/string-alphanumeric) 0 5)])
        time-gen (gen/double* {:infinite? false :NaN? false :min 0})
        ttl-gen (gen/one-of [(gen/return nil)
                             (gen/double* {:infinite? false :NaN? false :min 0})])
        make-riemann-event  (partial apply codec/->Event)]
    (->> (gen/tuple (gen/not-empty gen/string-alphanumeric)
                    (gen/not-empty gen/string-alphanumeric)
                    state-gen
                    nilable-string-gen
                    metric-gen
                    tags-gen
                    time-gen
                    ttl-gen)
         (gen/fmap make-riemann-event))))

(def clean-number-gen
  (gen/such-that Double/isFinite (gen/one-of [gen/small-integer gen/double])))

(def generic-event-gen
  (gen/map gen/keyword (gen/one-of [gen/string clean-number-gen gen/keyword gen/boolean])))

(def active-riemann-event-gen
  "Generator for events that might show up in active-riemann."
  (gen/one-of [riemann-event-gen
               generic-event-gen]))

(ct/defspec normalize-event-converts-valid-inputs-to-maps
  (prop/for-all [event active-riemann-event-gen]
                (let [normalized-event (active-index/normalize-event event)]
                  (and (map? normalized-event)
                       (not (record? normalized-event))))))

(ct/defspec try-store->try-read-edn-roundtrips-cleanly
  (prop/for-all [events (gen/vector active-riemann-event-gen)]
                (with-tmp-dir tmp-dir
                  (let [file-path (format "%s/riemann-index-backup-%s.edn" tmp-dir (gensym))]
                    (active-index/try-store file-path events)
                    (let [read-events (active-index/try-open-edn file-path)]
                      ;; Read: Normalizing events beforehand is the same as
                      ;; writing, then reading the unnormalized events to/from
                      ;; disk.
                      (= (mapv active-index/normalize-event events)
                         read-events))))))
