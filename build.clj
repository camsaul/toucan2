(ns build
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'io.github.camsaul/toucan2)

;;; if you want a version of MAJOR.MINOR.COMMITS:
(def version (format "0.9.%s" (b/git-count-revs nil)))

(def options
  {:lib lib, :version version})

(println "Options:" (pr-str options))

(defn build [& _]
  (bb/clean options)
  (bb/jar options))

(defn deploy [& _]
  (bb/deploy options))
