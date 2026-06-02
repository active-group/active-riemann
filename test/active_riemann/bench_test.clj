(ns active-riemann.bench-test
  (:require [active-riemann.bench :as bench]
            [clojure.test :as t]))

(t/deftest benchmark-test
  ;; Note: only tests that benchmarks can be done (don't fail)
  ;; The actual longer-running benchmarks should not be done as a unit test.

  ;; Note: should run the same tasks as /benchmark.sh, but with --dry-run added to not produce garbage in tests.
  (try (bench/-main "{:task :backup :number-of-events 1 :number-of-event-key-values 1}" "--dry-run")
       (bench/-main "{:task :restore :number-of-events 1 :number-of-event-key-values 1 :f-insertion? :dummy}" "--dry-run")
       (bench/-main "{:task :restore :number-of-events 1 :number-of-event-key-values 1 :f-insertion? :util}" "--dry-run")
       (bench/-main "{:task :restore :number-of-events 1 :number-of-event-key-values 1 :f-insertion? :enhanced}" "--dry-run")
       (t/is true)
       (catch Exception e
         (t/is false e))))
