(ns toucan2.jdbc
  "The Toucan 2 `next.jdbc` query execution backend."
  (:require
   [toucan2.util :as u]))

(defonce ^{:doc "Default options automatically passed to all `next.jdbc` queries and builder functions. This is stored
  as an atom; `reset!` or `swap!` it to define other default options."} global-options
  (atom {:label-fn u/lower-case-en}))

(def ^:dynamic *options*
  "Options to pass to `next.jdbc` when executing queries or statements. Overrides the [[global-options]]."
  nil)

(defn merge-options
  "Merge maps of `next.jdbc` options together. `extra-options` are ones passed in as part of the query execution pipeline
  and override [[*options*]], which in turn override the default [[global-options]]."
  [extra-options]
  (merge @global-options
         *options*
         extra-options))
