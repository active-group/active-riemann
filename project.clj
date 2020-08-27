(defproject de.active-group/active-riemann "0.5.0"
  :description "Common functionality for Riemann streams."
  :url "https://github.com/active-group/active-riemann"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [riemann "0.3.5"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
