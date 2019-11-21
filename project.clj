(defproject fourcan "1.0.0"
  :dependencies
  [
   [honeysql "0.9.5" :exclusions [org.clojure/clojurescript]]
   [methodical "0.9.4-alpha"]
   [potemkin "0.4.5"]
   [pretty "1.0.1"]
   [org.clojure/core.async "0.4.500"]
   [org.clojure/java.jdbc "0.7.9"]

   ]

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [com.h2database/h2 "1.4.197"]
     [org.clojure/tools.reader "1.1.1"]]}})
