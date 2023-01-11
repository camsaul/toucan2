(ns toucan2.jdbc.result-set
  "Implementation of a custom [[next.jdbc]] result set builder function, [[builder-fn]], and the default
  implementation; [[reduce-result-set]] which is used to reduce results from JDBC databases."
  (:require
   [better-cond.core :as b]
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [toucan2.instance :as instance]
   [toucan2.jdbc :as jdbc]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.jdbc.row :as jdbc.row]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet ResultSetMetaData)))

(set! *warn-on-reflection* true)

(m/defmulti builder-fn
  {:arglists '([^java.sql.Connection conn₁ model₂ ^java.sql.ResultSet rset opts])}
  u/dispatch-on-first-two-args)

(defrecord ^:no-doc InstanceBuilder [model ^ResultSet rset ^ResultSetMetaData rsmeta cols]
  next.jdbc.rs/RowBuilder
  (->row [_this]
    (log/tracef :results "Fetching row %s" (.getRow rset))
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
    (log/tracef :results "Converting transient row to persistent row")
    (persistent! row))

  next.jdbc.rs/ResultSetBuilder
  (->rs [_this]
    (transient []))

  (with-row [_this acc row]
    (conj! acc row))

  (rs! [_this acc]
    (persistent! acc)))

(defn- make-column-name->index [cols label-fn]
  {:pre [(seq cols) (fn? label-fn)]}
  (memoize
   (fn [column-name]
     (when (or (string? column-name)
               (instance? clojure.lang.Named column-name))
       (let [column-name' (keyword
                           (when (instance? clojure.lang.Named column-name)
                             (when-let [col-ns (namespace column-name)]
                               (label-fn (name col-ns))))
                           (label-fn (name column-name)))
             i            (when column-name'
                            (first (keep-indexed
                                    (fn [i col]
                                      (when (= col column-name')
                                        (inc i)))
                                    cols)))]
         (log/tracef :results "Index of column named %s (originally %s) is %s" column-name' column-name i)
         i)))))

(defn instance-builder-fn
  "Create a result set map builder function appropriate for passing as the `:builder-fn` option to [[next.jdbc]] that
  will create [[toucan2.instance]]s of `model` using namespaces determined
  by [[toucan2.model/table-name->namespace]]."
  [model ^ResultSet rset opts]
  (let [table-name->ns (model/table-name->namespace model)
        _              (log/debugf :results "Using table namespaces %s" table-name->ns)
        label-fn       (get opts :label-fn name)
        qualifier-fn   (memoize
                        (fn [table]
                          (let [table    (label-fn (name table))
                                table-ns (some-> (get table-name->ns table) name)]
                            (log/tracef :results "Using namespace %s for columns in table %s" table-ns table)
                            table-ns)))
        opts           (merge {:label-fn     label-fn
                               :qualifier-fn qualifier-fn}
                              opts)
        rsmeta         (.getMetaData rset)
        col-names      (next.jdbc.rs/get-modified-column-names rsmeta opts)]
    (log/tracef :results "Column names: %s" col-names)
    (constantly
     (assoc (->InstanceBuilder model rset rsmeta col-names) :opts opts))))

(m/defmethod builder-fn :default
  [_conn model rset opts]
  (let [merged-opts (jdbc/merge-options opts)]
    (instance-builder-fn model rset merged-opts)))

(defn reduce-result-set [rf init conn model ^ResultSet rset opts]
  (log/debugf :execute "Reduce JDBC result set for model %s with rf %s and init %s" model rf init)
  (let [row-num->i->thunk (jdbc.read/make-cached-row-num->i->thunk conn model rset)
        builder-fn*       (next.jdbc.rs/builder-adapter
                           (builder-fn conn model rset opts)
                           (jdbc.read/read-column-by-index-fn row-num->i->thunk))
        builder           (builder-fn* rset opts)
        combined-opts     (jdbc/merge-options (merge (:opts builder) opts))
        label-fn          (get combined-opts :label-fn)
        _                 (assert (fn? label-fn) "Options must include :label-fn")
        col-names         (get builder :cols (next.jdbc.rs/get-modified-column-names
                                              (.getMetaData rset)
                                              combined-opts))
        col-name->index   (make-column-name->index col-names label-fn)]
    (loop [acc init]
      (b/cond
        (not (.next rset))
        acc

        :let [row-num  (.getRow rset)
              i->thunk (row-num->i->thunk row-num)
              row      (jdbc.row/row model rset builder i->thunk col-name->index)
              acc'     (rf acc row)]

        (reduced? acc')
        @acc'

        :else
        (recur acc')))))
