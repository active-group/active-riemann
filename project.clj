(defproject de.active-group/active-riemann "0.13.0-SNAPSHOT"
  :description "Common functionality for Riemann streams."
  :url "https://github.com/active-group/active-riemann"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [de.active-group/active-clojure "0.45.1"]
                 [de.active-group/active-logger "0.16.4" :exclusions [com.fzakaria/slf4j-timbre]]
                 [riemann "0.3.12"]
                 [diehard "0.12.0"]]
  :jvm-opts ["-server"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
