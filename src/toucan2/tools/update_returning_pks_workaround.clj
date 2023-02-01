(ns toucan2.tools.update-returning-pks-workaround
  "Some databases like MySQL and MariaDB don't support returning PKs for UPDATE, so we'll have to hack it as follows:

  1. Rework the original query to be a SELECT, run it, and record the matching PKs somewhere. Currently only supported
     for queries we can manipulate e.g. Honey SQL

  2. Run the original UPDATE query

  3. Return the PKs from the rewritten SELECT query


  Since this is somewhat less efficient and doesn't work with raw SQL queries, you have to opt in to using it with one
  of the following methods:

  1. `reset!` the atom [[toucan2.tools.update-returning-pks-workaround/global-use-update-returning-pks-workaround]] to
     `true`

  2. Bind [[toucan2.tools.update-returning-pks-workaround/*use-update-returning-pks-workaround*]] to `true`"
  (:require
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]))

(defonce ^{:doc "Global default value for whether or not we should use the update-returning-pks workaround for databases
  like MySQL. You can temporarily override this by binding [[*use-update-returning-pks-workaround*]]."}
  global-use-update-returning-pks-workaround
  (atom false))

(def ^:dynamic *use-update-returning-pks-workaround*
  "Whether or not we should use the update-returning-pks workaround for databases like MySQL.
  Overrides [[global-use-update-returning-pks-workaround]] when set."
  nil)

(defn use-update-returning-pks-workaround?
  "Should we use the update-returning-pks workaround for databases like MySQL?"
  []
  (if (some? *use-update-returning-pks-workaround*)
    *use-update-returning-pks-workaround*
    @global-use-update-returning-pks-workaround))

(def ^:private ^:dynamic *pks* nil)

(derive ::update.capture-pks                :toucan.query-type/abstract)
(derive :toucan.query-type/update.pks       ::update.capture-pks)
(derive :toucan.query-type/update.instances ::update.capture-pks)

(m/defmethod pipeline/transduce-query [#_query-type     ::update.capture-pks
                                       #_model          :default
                                       #_resolved-query :default]
  "For databases that don't support returning PKs for UPDATE, rewrite the query as a SELECT and execute that to capture
  the PKs of the rows that will be affected. We need to capture PKs for both `:toucan.query-type/update.pks` and for
  `:toucan.query-type/update.instances`, since ultimately the latter is implemented on top of the former.See docstring
  for [[toucan2.tools.update-returning-pks-workaround]] for more information."
  [original-rf query-type model parsed-args conditions-map]
  (if-not (use-update-returning-pks-workaround?)
    (next-method original-rf query-type model parsed-args conditions-map)
    (do
      (log/debugf :execute "update-returning-pks-workaround: doing SELECT with conditions %s" conditions-map)
      (let [parsed-args (update parsed-args :kv-args merge conditions-map)
            select-rf   (pipeline/with-init conj [])
            xform       (map (model/select-pks-fn model))
            pks         (pipeline/transduce-query (xform select-rf)
                                                  :toucan.query-type/select.instances.fns
                                                  model
                                                  parsed-args
                                                  {})]
        (log/debugf :execute "update-returning-pks-workaround: got PKs %s" pks)
        (binding [*pks* pks]
          (next-method original-rf query-type model parsed-args conditions-map))))))

(m/defmethod pipeline/transduce-execute-with-connection [#_connection java.sql.Connection
                                                         #_query-type :toucan.query-type/update.pks
                                                         #_model      :default]
  [rf conn query-type model sql-args]
  (if-not (seq *pks*)
    (next-method rf conn query-type model sql-args)
    (do
      (let [update-rf (pipeline/default-rf :toucan.query-type/update.update-count)]
        (log/debugf :results "update-returning-pks-workaround: performing original UPDATE")
        (pipeline/transduce-execute-with-connection update-rf conn :toucan.query-type/update.update-count model sql-args))
      (log/debugf :results "update-returning-pks-workaround: transducing PKs with original reducing function")
      (transduce
       identity
       rf
       *pks*))))
