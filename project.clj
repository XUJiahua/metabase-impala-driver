(defproject metabase/impala-driver "1.0.0-SNAPSHOT-2.6.17"
  :min-lein-version "2.5.0"

  :dependencies [[impala-jdbc42 "2.6.17"]]
  ;; NOTE: make install-local-jar first,
  ;; install local impala driver first
  ;; https://github.com/kumarshantanu/lein-localrepo
  :plugins [[lein-localrepo "0.5.4"]]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "impala.metabase-driver.jar"}})
