(defproject de.active-group/active-riemann "0.14.0-SNAPSHOT"
  :description "Common functionality for Riemann streams."
  :url "https://github.com/active-group/active-riemann"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [de.active-group/active-clojure "0.45.1"]
                 [de.active-group/active-logger "0.16.4" :exclusions [com.fzakaria/slf4j-timbre]]
                 [riemann "0.3.12"]
                 [diehard "0.12.0"]
                 [com.magnars/test-with-files "2021-02-17"]]

  :jvm-opts ["-server"]
  :target-path "target/%s"
  :test-paths ["test" "bench"]
  :profiles {:uberjar {:aot :all}
             :test
             {:dependencies [[criterium "0.4.6"]
                             [riemann-clojure-client "0.5.4"]
                             [org.clojure/test.check "1.1.3"]]}
             :bench
             {:dependencies [[criterium "0.4.6"]]
              :source-paths ["src" "bench"]
              :main active-riemann.bench}}
  :aliases {"bench" ["with-profile" "bench" "run"]})
