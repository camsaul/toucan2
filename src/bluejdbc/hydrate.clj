(ns bluejdbc.hydrate
  (:require [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti hydrate*
  {:arglists '([table rows k])}
  u/dispatch-on-first-arg)

(defn hydrate [row-or-rows k & more])
