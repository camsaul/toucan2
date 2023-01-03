(ns toucan2.tools.default-fields
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(s/def ::default-field
  (s/or :keyword      keyword?
        :fn-and-alias (s/spec (s/cat :fn      ifn?
                                     :keyword keyword?))))

(s/def ::default-fields
  (s/coll-of ::default-field))

(m/defmulti default-fields
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod default-fields :around :default
  [model]
  (let [fields (next-method model)]
    (when (s/invalid? (s/conform ::default-fields fields))
      (throw (ex-info (format "Invalid default fields for %s: %s" (pr-str model) (s/explain-str ::default-fields fields))
                      (s/explain-data ::default-fields fields))))
    (log/debugf :results "Default fields for %s: %s" model fields)
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
           (into (empty instance) (map (fn [field-fn]
                                         (field-fn instance))
                                       field-fns))))))

(m/defmethod pipeline/transduce-with-model :around [#_query-type :toucan.result-type/instances
                                                    #_model      ::default-fields]
  [rf query-type model parsed-args]
  (log/debugf :results "Model %s has default fields" model)
  (cond
    ;; don't apply default fields for queries that specify other columns e.g. `(select [SomeModel :col])`
    (seq (:columns parsed-args))
    (do
      (log/debugf :results "Not adding default fields transducer since query already has `:columns`")
      (next-method rf query-type model parsed-args))

    ;; don't apply default fields for queries like [[toucan2.select/select-fn-set]] since they are already doing their
    ;; own transforms
    (isa? query-type :toucan.query-type/select.instances.fns)
    (do
      (log/debugf :results "Not adding default fields transducer since query type derives from :toucan.query-type/select.instances.fns")
      (next-method rf query-type model parsed-args))

    :else
    (do
      (log/debugf :results "adding transducer to return default fields for %s" model)
      (let [xform (default-fields-xform model)
            rf'   (xform rf)]
        (next-method rf' query-type model parsed-args)))))

(defmacro define-default-fields {:style/indent :defn} [model & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))

(s/fdef define-default-fields
  :args (s/cat :model some?
               :body  (s/+ any?))
  :ret any?)
