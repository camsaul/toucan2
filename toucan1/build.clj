(ns build
  (:require [clojure.string :as str]
            [org.corfield.build :as bb]))

(def lib 'io.github.camsaul/toucan2-toucan1)

(def major-minor-version (str/trim (slurp "../VERSION.txt")))

(defn commit-number []
  (or (-> (sh/sh "git" "rev-list" "HEAD" "--count")
          :out
          str/trim
          parse-long)
      "9999-SNAPSHOT"))

(def version (str major-minor-version \. (commit-number)))

(def options
  {:lib lib, :version version})

(println "Options:" (pr-str options))

(defn build [& _]
  (bb/clean options)
  (bb/jar options))

(defn deploy [& _]
  (bb/deploy options))
