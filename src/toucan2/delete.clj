(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [toucan2.pipeline :as pipeline]))

;;;; Code for building Honey SQL for DELETE lives in [[toucan2.map-backend.honeysql2]]

(defn reducible-delete
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/delete.update-count unparsed-args))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/delete.update-count unparsed-args))
