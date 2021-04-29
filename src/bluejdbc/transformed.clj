(ns bluejdbc.transformed
  (:require [bluejdbc.log :as log]
            [bluejdbc.select :as select]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti transforms*
  {:arglists '([connectable tableable options])}
  u/dispatch-on-first-two-args)

(m/defmethod transforms* :default
  [_ _ _]
  nil)

(defn- apply-transforms-to-honeysql [query transforms]
  ;; TODO
  query)

(defn conditions-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (let [transform-fns (for [[k {f :in}] transforms]
                        (fn [conditions]
                          (if (contains? conditions k)
                            (update conditions k f)
                            conditions)))]
    (apply comp transform-fns)))

(m/defmethod select/parse-select-args* :after [:default :bluejdbc/transformed]
  [connectable tableable {conditions :kvs, :as args} options]
  (let [transforms (not-empty (transforms* connectable tableable options))]
    (if (and transforms (seq conditions))
      (do
        (log/tracef "Apply %s transforms to conditions %s"
                    (pr-str (zipmap (keys transforms) (map :in (vals transforms))))
                    (pr-str conditions))
        (let [transform-fn (conditions-transform-fn transforms)]
          (update args :kvs transform-fn)))
      args)))

(m/defmethod select/select* :before [:default :bluejdbc/transformed clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (let [transforms (not-empty (transforms* connectable tableable options))]
    (cond-> query
      transforms (apply-transforms-to-honeysql transforms))))

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
