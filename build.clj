(ns build
  (:require [clojure.string :as str]
            [org.corfield.build :as bb]))

(def scm-url "git@github.com:camsaul/toucan2.git")
(def lib     'io.github.camsaul/toucan2)
(def version (str/trim (slurp "VERSION.txt")))

(def options
  {:lib     lib
   :version version
   :scm     {:tag                 (System/getenv "GITHUB_SHA")
             :connection          (str "scm:git:" scm-url)
             :developerConnection (str "scm:git:" scm-url)
             :url                 scm-url}})

(println "Options:" (pr-str options))

(defn build [& _]
  (bb/clean options)
  (bb/jar options))

(defn deploy [& _]
  (bb/deploy options))
