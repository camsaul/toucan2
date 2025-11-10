(ns toucan2.tools.default-fields
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.honeysql2 :as t2.honeysql]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(comment types/keep-me)

(set! *warn-on-reflection* true)

(s/def ::default-field
  (s/or :keyword      keyword?
        :fn-and-alias (s/spec (s/cat :fn      ifn?
                                     :keyword keyword?))))

(s/def ::default-fields
  (s/coll-of ::default-field))

(m/defmulti default-fields
  "The default fields to return for a model `model` that derives from `:toucan2.tools.default-fields/default-fields`. You
  probably don't need to use this directly; use [[toucan2.tools.default-fields/define-default-fields]] instead."
  {:arglists            '([model])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod default-fields :around :default
  [model]
  (let [fields (next-method model)]
    (when (s/invalid? (s/conform ::default-fields fields))
      (throw (ex-info (format "Invalid default fields for %s: %s" (pr-str model) (s/explain-str ::default-fields fields))
                      (s/explain-data ::default-fields fields))))
    (log/debugf "Default fields for %s: %s" model fields)
    fields))

(defn- default-fields-xform [model]
  (let [field-fns (mapv (fn [[field-type v]]
                          (case field-type
                            :keyword      (fn [instance]
                                            [v (get instance v)])
                            :fn-and-alias (let [{k :keyword, f :fn} v]
                                            (fn [instance]
                                              [k (f instance)]))))
                        (s/conform ::default-fields (default-fields model)))]
    (map (fn [instance]
           (log/tracef "Selecting default-fields from instance")
           (into (empty instance) (map (fn [field-fn]
                                         (field-fn instance))
                                       field-fns))))))

(def ^:dynamic *skip-default-fields*
  "Whether to skip applying default Fields because the query already includes explicit fields, e.g. `:select` for a Honey
  SQL query."
  false)

;;; TODO -- should we skip default fields for a Query that has top-level `:union` or `:union-all`?
(m/defmethod pipeline/transduce-query [#_query-type          :toucan.result-type/instances
                                       #_model               ::default-fields
                                       #_resolved-query-type clojure.lang.IPersistentMap]
  "Skip default fields behavior for Honey SQL queries that contain `:select`. Bind [[*skip-default-fields*]] to `true`."
  [rf query-type model parsed-args honeysql]
  (if (t2.honeysql/include-default-select? honeysql)
    (next-method rf query-type model parsed-args honeysql)
    (binding [*skip-default-fields* true]
      (log/debugf "Not adding default fields because query already contains `:select` or `:select-distinct`")
      (next-method rf query-type model parsed-args honeysql))))

(m/defmethod pipeline/results-transform [#_query-type :toucan.result-type/instances #_model ::default-fields]
  [query-type model]
  (log/debugf "Model %s has default fields" model)
  (cond
    *skip-default-fields*
    (next-method query-type model)

    ;; don't apply default fields for queries that specify other columns e.g. `(select [SomeModel :col])`
    (seq (:columns pipeline/*parsed-args*))
    (do
      (log/debugf "Not adding default fields transducer since query already has `:columns`")
      (next-method query-type model))

    ;; don't apply default fields for queries like [[toucan2.select/select-fn-set]] since they are already doing their
    ;; own transforms
    (isa? query-type :toucan.query-type/select.instances.fns)
    (do
      (log/debugf "Not adding default fields transducer since query type derives from :toucan.query-type/select.instances.fns")
      (next-method query-type model))

    ;; don't apply default fields for the recursive select done by before-update, because it busts things when we want
    ;; to update non-default fields =(
    ;;
    ;; See [[toucan2.tools.before-update-test/before-update-with-default-fields-test]]
    (isa? query-type :toucan2.tools.before-update/select-for-before-update)
    (do
      (log/debugf "Not adding default fields transducer since query type is done for the purposes of before-update")
      (next-method query-type model))

    :else
    (do
      (log/debugf "adding transducer to return default fields for %s" model)
      (let [xform (default-fields-xform model)]
        (comp xform
              (next-method query-type model))))))

;;; `default-fields` should be done before [[toucan2.tools.after-select]] so that fields added by after select get
;;; preserved.
(m/prefer-method! #'pipeline/results-transform
                  [:toucan.result-type/instances ::default-fields]
                  [:toucan.result-type/instances :toucan2.tools.after-select/after-select])

;;; `default-fields` should be done before [[toucan2.tools.after-update]] and [[toucan2.tools.after-insert]] so that
;;; fields added by those methods get preserved.
(m/prefer-method! #'pipeline/results-transform
                  [:toucan.result-type/instances ::default-fields]
                  [:toucan.result-type/instances :toucan2.tools.after/model])

(defmacro define-default-fields {:style/indent :defn} [model & body]
  `(do
     (u/unparent-descendants ~model ::default-fields)
     (u/maybe-derive ~model ::default-fields)
     (m/defmethod default-fields ~model [~'&model] ~@body)))

(s/fdef define-default-fields
  :args (s/cat :model some?
               :body  (s/+ any?))
  :ret any?)
