(ns bluejdbc.transformed
  (:require [bluejdbc.log :as log]
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

(defn transform-conditions [conditions transforms]
  {:pre [(seq transforms)]}
  (into {} (for [[k v] conditions]
             [k (if-let [xform (get transforms k)]
                  (xform v)
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

(m/defmethod select/parse-select-args* :after [:default :bluejdbc/transformed]
  [connectable tableable {conditions :kvs, pk :id, :as args} options]
  (let [transforms (not-empty (transforms* connectable tableable options))]
    (if-not (and transforms (or (seq conditions) pk))
      args
      (let [transforms (zipmap (keys transforms) (map :in (vals transforms)))]
        (log/tracef "Apply %s transforms to pk %s and conditions %s" transforms pk conditions)
        (cond-> args
          (seq conditions) (update :kvs transform-conditions transforms)
          pk               (update :id transform-pk connectable tableable transforms))))))

#_(m/defmethod select/select* :before [:default :bluejdbc/transformed clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (let [transforms (not-empty (transforms* connectable tableable options))]
    (cond-> query
      transforms (transform-honeysql transforms))))

(defn row-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (let [transform-fns (for [[k {f :out}] transforms]
                        (fn [row]
                          (if (contains? row k)
                            (update row k f)
                            row)))]
    (apply comp transform-fns)))

(m/defmethod select/select* :after [:default :bluejdbc/transformed :default]
  [connectable tableable reducible-query options]
  (if-let [transforms (not-empty (transforms* connectable tableable options))]
    (do
      (log/tracef "Apply %s transforms %s to results" (pr-str tableable) (pr-str transforms))
      (eduction
       (map (row-transform-fn transforms))
       reducible-query))
    reducible-query))
