(ns fourcan.compile
  (:require [fourcan
             [debug :as debug]
             [hierarchy :as hierarchy]
             [types :as types]
             [util :as u]]
            [honeysql.core :as hsql]
            [honeysql.helpers :as h]
            [methodical.core :as m]))

(m/defmulti honeysql-options
  {:arglists '([model])}
  keyword
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod honeysql-options :default
  [_]
  {:quoting :ansi})

(m/defmulti table-name
  {:arglists '([k])}
  types/model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod table-name :default
  [k]
  (types/model k))

(m/defmulti primary-key
  {:arglists '([model])}
  types/model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod primary-key :default
  [_]
  :id)

(m/defmulti primary-key-where-clause
  {:arglists '([model pk-values])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod primary-key-where-clause :default
  [a-model pk-values]
  (let [pk-values (if (sequential? pk-values) pk-values [pk-values])
        pk-keys   (primary-key a-model)
        pk-keys   (if (sequential? pk-keys) pk-keys [pk-keys])]
    (when-not (= (count pk-keys) (count pk-values))
      (throw (Exception. (format "model %s expected %d primary key values %s, got %d values"
                                 (pr-str (types/model a-model)) (count pk-keys) pk-keys (count pk-values)))))
    (if (= (count pk-keys) 1)
      [:= (first pk-keys) (first pk-values)]
      (into [:and] (for [[k v] (zipmap pk-keys pk-values)]
                     [:= k v])))))

(m/defmulti compile-honeysql
  {:arglists '([model form])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod compile-honeysql :around :default
  [model form]
  (if debug/*debug*
    (do
      (printf "(compile-honeysql %s %s)\n" (pr-str model) (pr-str form))
      (let [result (next-method model form)]
        (printf ";; -> %s\n" (pr-str result))
        result))
    (next-method model form)))

;; TODO — add a debugging :before and :after methods
(m/defmethod compile-honeysql :default
  [model form]
  (cond
    (map? form)    (u/mapply hsql/format form (honeysql-options model))
    (string? form) [form]
    :else          form))

;; TODO - these all need to dispatch on model

(defn- where
  "Generate a HoneySQL `where` form using key-value args.

     (where {} :a :b)        -> (h/merge-where {} [:= :a :b])
     (where {} :a [:!= b])   -> (h/merge-where {} [:!= :a :b])"
  [honeysql-form k v]
  (let [where-clause (if (vector? v)
                       (let [[f & args] v]
                         (assert (keyword? f))
                         (into [f k] args))
                       [:= k v])]
    (h/merge-where honeysql-form where-clause)))

(defn compile-select-options [args]
  (loop [honeysql-form {}, [arg1 arg2 & more :as remaining] args]
    (cond
      (nil? arg1)
      honeysql-form

      (keyword? arg1)
      (recur (where honeysql-form arg1 arg2) more)

      (map? arg1)
      (recur (merge honeysql-form arg1) (cons arg2 more))

      :else
      (throw (ex-info (format "Don't know what to do with arg: %s. Expected keyword or map" arg1)
                      {:arg arg1, :all-args args})))))

(defn primary-key-value
  ([object]
   (when object
     (let [model (types/model object)
           pk    (primary-key model)]
       (select-keys object (if (sequential? pk) pk [pk])))))

  ([model m]
   (primary-key-value (types/instance model m))))
