(ns build
  (:require [clojure.string :as str]
            [org.corfield.build :as bb]))

(def lib 'io.github.camsaul/toucan2)

(def version (str/trim (slurp "VERSION.txt")))

(def options
  {:lib lib, :version version})

(println "Options:" (pr-str options))

(defn build [& _]
  (bb/clean options)
  (bb/jar options))

(defn deploy [& _]
  (bb/deploy options))
