(defproject active-riemann "0.1.0"
  :description "Common functionality for Riemann streams."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [riemann "0.3.5"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
