(ns bluejdbc.transformed
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.jdbc.row :as row]
            [bluejdbc.log :as log]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti transforms*
  {:arglists '([connectable tableable options])}
  u/dispatch-on-first-two-args)

(m/defmethod transforms* :default
  [_ _ _]
  nil)

;; I originally considered walking and transforming the HoneySQL, but decided against it because it's too ambiguous.
;; It's too hard to tell if
;;
;;    [:= :id :col]
;;
;; means
;;
;; A) `:id` is a column identifier key, and `:col` is a value for it that we should transform
;; B) `:col` is a column identifier key, and `:id` is a value for it that we should transform
;; C) `:id` is a column identifier key, but `:col` is just a reference to another column, and we shouldn't transform it
;; D) `:col` is a column identifier key, but `:id` is just a reference to another column, and we shouldn't transform it
;;
;; It's also hard to know what are the "values" of every different type of filter clause (including custom ones we
;; don't know about). I think leaving HoneySQL as an outlet to bypass type transforms makes sense for now. This also
;; avoids locking us in to HoneySQL too much

(defn- transform-value [xform v]
  (try
    (if (sequential? v)
      (into [(first v)]
            (map (fn xform* [v]
                   (if (sequential? v)
                     (mapv xform* v)
                     (xform v))))
            (rest v))
      (xform v))
    (catch Throwable e
      (throw (ex-info (format "Error transforming %s: %s" (pr-str v) (ex-message e))
                      {:v v, :transform xform}
                      e)))))

(defn transform-conditions [conditions transforms]
  {:pre [(seq transforms)]}
  (into {} (for [[k v] conditions]
             [k (if-let [xform (get transforms k)]
                  (transform-value xform v)
                  v)])))

(defn transform-pk [pk-vals connectable tableable transforms]
  (if-not (sequential? pk-vals)
    (first (transform-pk [pk-vals] connectable tableable transforms))
    (let [pk-keys (tableable/primary-key-keys connectable tableable)]
      (mapv
       (fn [k v]
         (if-let [xform (get transforms k)]
           (xform v)
           v))
       pk-keys
       pk-vals))))

(defn in-transforms [connectable tableable options]
  (when-let [transforms (not-empty (transforms* connectable tableable options))]
    ;; make the transforms map an instance so we can get appropriate magic map behavior when looking for the
    ;; appropriate transform for a given key.
    (instance/instance
     connectable tableable
     (zipmap (keys transforms) (map :in (vals transforms))))))

(defn apply-in-transforms
  [connectable tableable {:keys [conditions pk], :as args} options]
  (if-let [transforms (not-empty (when (or (seq conditions) pk)
                                   (in-transforms connectable tableable options)))]
    (log/with-trace ["Apply transforms to pk %s and conditions %s" pk conditions]
      (log/trace transforms)
      (cond-> args
        (seq conditions) (update :conditions transform-conditions transforms)
        pk               (update :pk transform-pk connectable tableable transforms)))
    args))

(m/defmethod select/parse-select-args* :after [:default :bluejdbc/transformed]
  [connectable tableable args options]
  (apply-in-transforms connectable tableable args options))

(defn apply-row-transform [instance k xform]
  ;; The "Special Optimizations" below *should* be the default case, but if some other aux methods are in place or
  ;; custom impls it might not be; things should still work normally either way.
  ;;
  ;; Special Optimization 1: if `instance` is an `IInstance`, and original and current are the same object, this only
  ;; applies `xform` once.
  (instance/update-original-and-current
   instance
   (fn [row]
     ;; Special Optimization 2: if the underlying original/current maps of `instance` are instances of `IRow` (which
     ;; themselves have underlying key->value thunks) we can compose the thunk itself rather than immediately
     ;; realizing and transforming the value. This means transforms don't get applied to values that are never
     ;; realized.
     (if-let [thunks (row/thunks row)]
       (row/with-thunks row (update thunks k (fn [thunk]
                                               (comp xform thunk))))
       (update row k xform)))))

(defn row-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (let [transform-fns (for [[k {xform :out}] transforms]
                        (fn [instance]
                          (if (contains? instance k)
                            (apply-row-transform instance k xform)
                            instance)))]
    (apply comp transform-fns)))

(defn transform-results [connectable tableable reducible-query options]
  (if-let [transforms (not-empty (transforms* connectable tableable options))]
    (log/with-trace ["Apply transforms %s to results" tableable]
      (log/trace transforms)
      (eduction
       (map (row-transform-fn transforms))
       reducible-query))
    reducible-query))

(m/defmethod select/select* :after [:default :bluejdbc/transformed :default]
  [connectable tableable reducible-query options]
  (transform-results connectable tableable reducible-query options))

(m/defmethod mutative/parse-update!-args* :after [:default :bluejdbc/transformed]
  [connectable tableable args options]
  (if-let [transforms (in-transforms connectable tableable options)]
    (log/with-trace ["Apply %s transforms to %s" transforms args]
      (cond-> (apply-in-transforms connectable tableable args options)
        transforms (update :changes transform-conditions transforms)))
    args))

(defn transform-insert-rows [[first-row :as rows] transforms]
  ;; all rows should have the same keys, so we just need to look at the keys in the first row
  (let [row-xforms (for [k (keys first-row)
                         :let [xform (get transforms k)]
                         :when xform]
                     (fn [row]
                       (update row k xform)))
        row-xform (apply comp row-xforms)]
    (map row-xform rows)))

(m/defmethod mutative/parse-insert!-args* :after [:default :bluejdbc/transformed]
  [connectable tableable {:keys [rows], :as args} options]
  (if-let [transforms (in-transforms connectable tableable options)]
    (log/with-trace ["Apply %s transforms to %s" transforms rows]
      (update args :rows transform-insert-rows transforms))
    args))

(m/defmethod mutative/insert!* :after [:default :bluejdbc/transformed :default]
  [connectable tableable results options]
  (if (integer? results)
    results
    (transform-results connectable tableable results options)))
