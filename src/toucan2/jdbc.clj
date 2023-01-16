(ns toucan2.jdbc
  (:require
   [toucan2.util :as u]))

(defonce ^{:doc "Default options automatically passed to all [[next.jdbc]] queries and builder functions."}
  global-options
  (atom {:label-fn u/lower-case-en}))

(def ^:dynamic *options*
  "Options to pass to [[next.jdbc]] when executing queries or statements."
  nil)

(defn merge-options [extra-options]
  (merge @global-options
         *options*
         extra-options))
