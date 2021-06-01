(ns bluejdbc.transformed
  (:require [bluejdbc.instance :as instance]
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
    (zipmap (keys transforms) (map :in (vals transforms)))))

(defn apply-in-transforms
  [connectable tableable {:keys [conditions pk], :as args} options]
  (if-let [transforms (when (or (seq conditions) pk)
                        (in-transforms connectable tableable options))]
    (log/with-trace ["Apply %s transforms to pk %s and conditions %s" transforms pk conditions]
      (cond-> args
        (seq conditions) (update :conditions transform-conditions transforms)
        pk               (update :pk transform-pk connectable tableable transforms)))
    args))

(m/defmethod select/parse-select-args* :after [:default :bluejdbc/transformed]
  [connectable tableable args options]
  (apply-in-transforms connectable tableable args options))

(defn row-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (let [transform-fns (for [[k {f :out}] transforms]
                        (fn [row]
                          (if (contains? row k)
                            (update row k f)
                            row)))]
    (apply comp instance/reset-original transform-fns)))

(m/defmethod select/select* :after [:default :bluejdbc/transformed :default]
  [connectable tableable reducible-query options]
  (if-let [transforms (not-empty (transforms* connectable tableable options))]
    (do
      (log/tracef "Apply %s transforms %s to results" (pr-str tableable) (pr-str transforms))
      (eduction
       (map (row-transform-fn transforms))
       reducible-query))
    reducible-query))

(m/defmethod mutative/parse-update!-args* :after [:default :bluejdbc/transformed]
  [connectable tableable {:keys [changes], :as args} options]
  (if-let [transforms (in-transforms connectable tableable options)]
    (log/with-trace ["Apply %s transforms to %s" transforms args]
      (cond-> (apply-in-transforms connectable tableable args options)
        transforms (update :changes transform-conditions transforms)))
    args))

(defn transform-insert-rows [rows transforms]
  (let [row-xforms (for [[k xform] transforms]
                     (fn [row]
                       (if (contains? row k)
                         (update row k xform)
                         row)))
        row-xform  (apply comp row-xforms)]
    (map row-xform rows)))

(m/defmethod mutative/parse-insert!-args* :after [:default :bluejdbc/transformed]
  [connectable tableable {:keys [rows], :as args} options]
  (if-let [transforms (in-transforms connectable tableable options)]
    (log/with-trace ["Apply %s transforms to %s" transforms rows]
      (update args :rows transform-insert-rows transforms))
    args))

(m/defmethod mutative/insert!* :after [:default :bluejdbc/transformed :default]
  [connectable tableable results options]
  (if-not (sequential? results)
    results
    (if-let [transforms (not-empty (transforms* connectable tableable options))]
      (log/with-trace ["Apply %s transforms %s to results" (pr-str tableable) (pr-str transforms)]
        (map (row-transform-fn transforms) results))
      results)))
