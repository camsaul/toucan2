(ns toucan2.delete
  "Implementation of [[delete!]].

  Code for building Honey SQL for DELETE lives in [[toucan2.map-backend.honeysql2]]"
  (:require
   [toucan2.pipeline :as pipeline]))

(defn delete!
  "Delete instances of a model that match `conditions` or a `queryable`. Returns the number of rows deleted.

  ```clj
  ;; delete a row by primary key
  (t2/delete! :models/venues 1)

  ;; Delete all rows matching a condition
  (t2/delete! :models/venues :name \"Bird Store\")
  ```

  Allowed syntax is identical to [[toucan2.select/select]], including optional positional parameters like `:conn`;
  see [[toucan2.query/parse-args]] and the `:toucan2.query/default-args` spec."
  {:arglists '([modelable & conditions? queryable?]
               [:conn connectable modelable & conditions? queryable?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/delete.update-count unparsed-args))
