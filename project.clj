(defproject de.active-group/active-riemann "0.8.0-SNAPSHOT"
  :description "Common functionality for Riemann streams."
  :url "https://github.com/active-group/active-riemann"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [de.active-group/active-clojure "0.35.2"]
                 [riemann "0.3.5"]
                 [diehard "0.10.0"]]
  :jvm-opts ["-server"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
