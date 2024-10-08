(ns toucan2.jdbc.result-set
  "Implementation of a custom `next.jdbc` result set builder function, [[builder-fn]], and the default
  implementation; [[reduce-result-set]] which is used to reduce results from JDBC databases."
  (:require
   [better-cond.core :as b]
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [toucan2.instance :as instance]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.jdbc.row :as jdbc.row]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.types :as types]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet ResultSetMetaData)))

(set! *warn-on-reflection* true)

(comment s/keep-me
         types/keep-me)

(m/defmulti builder-fn
  "Return the `next.jdbc` builder function to use to create the results when querying a model. By default, this
  uses [[instance-builder-fn]], and returns Toucan 2 instances; but if you want to use plain maps you can use one of the
  other builder functions that ships with `next.jdbc`, or write your own custom builder function."
  {:arglists            '([^java.sql.Connection conn₁ model₂ ^java.sql.ResultSet rset opts])
   :defmethod-arities   #{4}
   :dispatch-value-spec (types/or-default-spec
                         (s/cat :conn  ::types/dispatch-value.keyword-or-class
                                :model ::types/dispatch-value.model))}
  u/dispatch-on-first-two-args)

(defrecord ^:no-doc InstanceBuilder [model ^ResultSet rset ^ResultSetMetaData rsmeta cols]
  next.jdbc.rs/RowBuilder
  (->row [_this]
    (log/tracef "Fetching row %s" (.getRow rset))
    (transient (instance/instance model)))

  (column-count [_this]
    (count cols))

  ;; this is purposefully not implemented because we should never get here; if we do it is an error and we want an
  ;; Exception thrown.
  #_(with-column [this row i]
      (println (pr-str (list 'with-column 'this 'row i)))
      (next.jdbc.rs/with-column-value this row (nth cols (dec i))
        (next.jdbc.rs/read-column-by-index (.getObject rset ^Integer i) rsmeta i)))

  (with-column-value [_this row col v]
    (assert (some? col) "Invalid col")
    (assoc! row col v))

  (row! [_this row]
    (log/tracef "Converting transient row to persistent row")
    (persistent! row))

  next.jdbc.rs/ResultSetBuilder
  (->rs [_this]
    (transient []))

  (with-row [_this acc row]
    (conj! acc row))

  (rs! [_this acc]
    (persistent! acc)))

(defn- column-name->keyword [column-name label-fn]
  (when (or (string? column-name)
            (instance? clojure.lang.Named column-name))
    (keyword
     (when (instance? clojure.lang.Named column-name)
       (when-let [col-ns (namespace column-name)]
         (name (label-fn (name col-ns)))))
     (name (label-fn (name column-name))))))

(defn- make-column-name->index [cols label-fn]
  {:pre [(fn? label-fn)]}
  (if (empty? cols)
    (constantly nil)
    (memoize
     (fn [column-name]
       ;; TODO FIXME -- it seems like the column name we get here has already went thru the label fn/qualifying
       ;; functions. The `(originally ...)` in the log message is wrong. Are we applying label function twice?!
       (let [column-name' (column-name->keyword column-name label-fn)
             i            (when column-name'
                            (first (keep-indexed
                                    (fn [i col]
                                      (when (= col column-name')
                                        (inc i)))
                                    cols)))]
         (log/tracef "Index of column named %s (originally %s) is %s" column-name' column-name i)
         (when-not i
           (log/debugf "Could not determine index of column name %s. Found: %s" column-name cols))
         i)))))

(defn instance-builder-fn
  "Create a result set map builder function appropriate for passing as the `:builder-fn` option to `next.jdbc` that
  will create [[toucan2.instance]]s of `model` using namespaces determined
  by [[toucan2.model/table-name->namespace]]."
  [model ^ResultSet rset opts]
  (let [table-name->ns (model/table-name->namespace model)
        label-fn       (get opts :label-fn name)
        qualifier-fn   (memoize
                        (fn [table]
                          (let [table    (some-> table not-empty name label-fn)
                                table-ns (some-> (get table-name->ns table) name)]
                            (log/tracef "Using namespace %s for columns in table %s" table-ns table)
                            table-ns)))
        opts           (merge {:label-fn     label-fn
                               :qualifier-fn qualifier-fn}
                              opts)
        rsmeta         (.getMetaData rset)
        _              (log/debugf "Getting modified column names with next.jdbc options %s" opts)
        col-names      (next.jdbc.rs/get-modified-column-names rsmeta opts)]
    (log/debugf "Column names: %s" col-names)
    (constantly
     (assoc (->InstanceBuilder model rset rsmeta col-names) :opts opts))))

(m/defmethod builder-fn :default
  "Default `next.jdbc` builder function. Uses [[instance-builder-fn]] to return Toucan 2 instances."
  [_conn model rset opts]
  (let [merged-opts (jdbc.options/merge-options opts)]
    (instance-builder-fn model rset merged-opts)))

(defn ^:no-doc reduce-result-set
  "Reduce a `java.sql.ResultSet` using reducing function `rf` and initial value `init`. `conn` is an instance of
  `java.sql.Connection`. `conn` and `model` are used mostly for dispatch value purposes for things like [[builder-fn]],
  and for creating instances with the correct model.

  Part of the low-level implementation of the JDBC query execution backend -- you probably shouldn't be using this
  directly."
  [rf init conn model ^ResultSet rset opts]
  (log/debugf "Reduce JDBC result set for model %s with rf %s and init %s" model rf init)
  (let [i->thunk          (jdbc.read/make-i->thunk conn model rset)
        builder-fn*       (next.jdbc.rs/builder-adapter
                           (builder-fn conn model rset opts)
                           (jdbc.read/read-column-by-index-fn i->thunk))
        builder           (builder-fn* rset opts)
        combined-opts     (jdbc.options/merge-options (merge (:opts builder) opts))
        label-fn          (get combined-opts :label-fn)
        _                 (assert (fn? label-fn) "Options must include :label-fn")
        col-names         (get builder :cols (next.jdbc.rs/get-modified-column-names
                                              (.getMetaData rset)
                                              combined-opts))
        col-names-kw      (into [] (keep #(column-name->keyword % label-fn)) col-names)
        col-name->index   (make-column-name->index col-names label-fn)]
    (log/tracef "column name -> index = %s" col-name->index)
    (loop [acc init]
      (b/cond
        (not (.next rset))
        (do
          (log/tracef "Result set has no more rows.")
          acc)

        :let [_        (log/tracef "Fetch row %s" (.getRow rset))
              row      (jdbc.row/row model rset builder i->thunk col-name->index col-names-kw)
              acc'     (rf acc row)]

        (reduced? acc')
        @acc'

        :else
        (recur acc')))))
