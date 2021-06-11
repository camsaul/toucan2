(ns bluejdbc.helpers
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.transformed :as transformed]
            [bluejdbc.util :as u]
            [clojure.walk :as walk]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(defn dispatch-value-2 [dispatch-value]
  (let [{:keys [connectable tableable]}
        (if-not (sequential? dispatch-value)
          {:tableable dispatch-value}
          (zipmap
           (condp = (count dispatch-value)
             1 [:tableable]
             [:connectable :tableable])
           dispatch-value))]
    [(or connectable :default)
     (or tableable :default)]))

(defn dispatch-value-3 [dispatch-value]
  (let [{:keys [connectable tableable x]}
        (if-not (sequential? dispatch-value)
          {:tableable dispatch-value}
          (zipmap
           (condp = (count dispatch-value)
             1 [:tableable]
             2 [:connectable :tableable]
             [:connectable :tableable :x])
           dispatch-value))]
    [(or connectable :default)
     (or tableable :default)
     (or x :default)]))

(defn do-helper [msg tableable thunk]
  (log/with-trace ["Doing %s for %s" msg tableable]
    (try
      (thunk)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" msg (pr-str tableable) (ex-message e))
                        {:tableable tableable}
                        e))))))

(defmacro helper {:style/indent 2} [msg tableable & body]
  `(do-helper ~msg ~tableable (fn [] ~@body)))

(defmacro define-table-name {:style/indent :defn} [dispatch-value & body]
  `(m/defmethod tableable/table-name* ~(dispatch-value-2 dispatch-value)
     [~'&connectable ~'&tableable ~'&options]
     ~@body))

(defn do-before-select
  [connectable tableable query options f]
  (helper "before-select" tableable
    (f query)))

(defmacro define-before-select {:style/indent :defn} [dispatch-value [query-binding] & body]
  `(m/defmethod select/select* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-select ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~query-binding]
                         ~@body))))

(defn do-after-select [connectable tableable reducible-query options f]
  (helper "after-select" tableable
    (eduction
     (map f)
     reducible-query)))

(defmacro define-after-select {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod select/select* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&reducible-query ~'&options]
     (do-after-select ~'&connectable ~'&tableable ~'&reducible-query ~'&options
                      (fn [~instance-binding]
                        ~@body))))

;; HELPERS BELOW ARE EXPERIMENTAL/UNTESTED

(m/defmulti before-update-transform-changes*
  {:arglists '([connectableᵈ instanceᵈᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :second))

;; default method is a no-op
(m/defmethod before-update-transform-changes* :default
  [_ instance _]
  instance)

(defn reducible-instances-matching-update-query
  "Return instances of `tableable` that match conditions from a compiled `update-query` as passed to `update!*`."
  [connectable tableable update-query options]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        select-query          (-> update-query
                                  (dissoc :update :set)
                                  (assoc :from [(compile/table-identifier tableable options)]))]
    (log/with-trace-no-result ["Finding matching instances with query %s" select-query]
      (select/select-reducible [connectable tableable] select-query (or options {})))))

(defn group-by-xform
  "Transducer that groups together values into a map of `(key-fn x)` -> `[(val-fn x) ...]`, then reduces that map.

    (into {} (group-by-xform even? inc) [1 2 3 4])
    ;; ->
    {false [2 4], true [3 5]}"
  [key-fn val-fn]
  (fn [rf]
    (let [k->v (atom {})]
      (fn
        ([] (rf))
        ([acc]
         ;; optimization: remove the matching-primary-keys if there's just one set of changes to apply.
         (let [k->v @k->v
               k->v (if (= (bounded-count 2 k->v) 1)
                      {(ffirst k->v) nil}
                      k->v)]
           (reduce
            rf
            acc
            k->v)))
        ([acc x]
         (let [k (key-fn x)
               v (val-fn x)]
           (swap! k->v update k #(conj (vec %) v))
           acc))))))

#_(def ^:dynamic *update-batch-size*
  "Maximum number of rows to fetch and update at a time when using `define-before-update` and
  `::before-update-transform-matching-rows`."
  100)

(m/defmethod mutative/update!* :around [:default ::before-update-transform-matching-rows clojure.lang.IPersistentMap]
  [connectable tableable {updates :set, :as query} options]
  (log/with-trace-no-result ["Doing before-update for %s" tableable]
    (if (empty? updates)
      (do (log/tracef "Query %s has no changes; skipping rest of update" query)
          0)
      (letfn [(f [instance]
                (before-update-transform-changes* connectable instance options))]
        ;; TODO -- this whole thing should be done in a transaction.
        (transduce
         (comp
          ;; merge in changes for each instance
          (map (fn [instance]
                 (log/tracef "Found matching instance %s" instance)
                 (merge instance updates)))
          ;; filter out instances that don't have any changes before calling `f`
          (filter (fn [instance]
                    (if (seq (instance/changes instance))
                      instance
                      (log/tracef "Skipping row with PK %s, it has no changes" (tableable/primary-key-values instance)))))
          ;; apply the changes-xform to each instance
          (map (fn [instance]
                 (log/with-trace ["Apply f to %s" instance]
                   (let [result (f instance)]
                     (assert (instance/bluejdbc-instance? result)
                             (format "before-update method for %s should return an instance, got ^%s %s"
                                     (u/dispatch-value tableable)
                                     (some-> result class .getCanonicalName)
                                     (pr-str result)))
                     result))))
          ;; filter out the ones that don't have any changes AFTER calling `f`.
          (filter (fn [instance]
                    (if (seq (instance/changes instance))
                      instance
                      (log/tracef "Skipping row with PK %s, it has no changes after applying f"
                                  (tableable/primary-key-keys connectable instance)))))
          ;; TODO -- consider whether we should batch the stuff below. e.g. if we end up matching 1 million objects,
          ;; it might not be ideal to keep a million PK value vectors in memory at once. Also, a query with `UPDATE
          ;; table WHERE id IN (...)` with a million ids probably isn't going to work so well.
          #_(partition-all *update-batch-size*)
          ;; Group all the PKs by their changes.
          ;;
          ;; TODO -- if we had a batched-update method, we wouldn't need this complicated transducer.
          (group-by-xform instance/changes (let [pk-keys (vec (tableable/primary-key-keys connectable tableable))]
                                             #(mapv % pk-keys)))
          ;; do an update call for each distinct set of changes
          (map (fn [[changes matching-primary-keys]]
                 (let [new-query (-> query
                                     (assoc :set changes)
                                     (honeysql-util/merge-conditions connectable tableable
                                                                     {:bluejdbc/with-pks matching-primary-keys}
                                                                     options))]
                   (log/with-trace ["Performing updates with query %s" new-query]
                     (next-method connectable tableable new-query options))))))
         (completing (fnil + 0 0))
         0
         (reducible-instances-matching-update-query connectable tableable query options))))))

(defn disallow-next-method-calls [body]
  (walk/postwalk
   (fn [form]
     (if (and (sequential? form)
              (= (first form) 'next-method))
       (throw (ex-info "Don't call next-method here. It's already called automatically!"
                       {:body body, :form form}))
       form))
   body))

(defmacro define-before-update {:style/indent :defn} [dispatch-value [instance-binding] & body]
  (let [[connectable-dv tableable-dv] (dispatch-value-2 dispatch-value)]
    `(do
       (when-not (isa? ~tableable-dv ::before-update-transform-matching-rows)
         (derive ~tableable-dv ::before-update-transform-matching-rows))
       (m/defmethod before-update-transform-changes* [~connectable-dv ~tableable-dv]
         [~'&connectable ~instance-binding ~'&options]
         (let [result# ~(disallow-next-method-calls `(do ~@body))]
           (~'next-method ~'&connectable result# ~'&options))))))

(defn do-after-update [connectable tableable result options f]
  (helper "after-update" tableable
    (f result)))

(defmacro define-after-update {:style/indent :defn} [dispatch-value [result-binding] & body]
  `(m/defmethod mutative/update!* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&result ~'&options]
     (do-after-update ~'&connectable ~'&tableable ~'&result ~'&options
                      (fn [~result-binding]
                        ~@body))))

(defn do-before-insert [connectable tableable query options f]
  (helper "before-insert" tableable
    (update query :values (fn [rows]
                            (mapv
                             (fn [row]
                               (f (instance/instance connectable tableable row)))
                             rows)))))

(defmacro define-before-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/insert!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-insert ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~instance-binding]
                         ~@body))))

(defn do-after-insert [connectable tableable query options next-method f]
  (helper "after-insert" tableable
    (let [reducible-query (next-method connectable tableable query (-> options
                                                                       (assoc :reducible? true)
                                                                       (assoc-in [:next.jdbc :return-keys] true)))
          pks             (into [] (map (select/select-pks-fn connectable tableable)) reducible-query)
          instances       (select/select [connectable tableable]
                                         :bluejdbc/with-pks pks
                                         {}
                                         (update options :next.jdbc dissoc :return-keys))]
      (mapv f instances))))

(defmacro define-after-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/insert!* :around ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-after-insert ~'&connectable ~'&tableable ~'&query ~'&options ~'next-method
                      (fn [~instance-binding]
                        ~@body))))

(defn do-before-delete [connectable tableable query options f]
  (helper "before-delete" tableable
    (let [instances (select/select [connectable tableable] (select-keys query [:where]))]
      (doseq [instance instances]
        (f instance)))))

(defmacro define-before-delete {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/delete!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-delete ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~instance-binding]
                         ~@body))))

;; TODO
#_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
    (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
      [~'&connectable ~'&tableable _ ~'&options]
      ~@body))

(defmacro define-hydration-keys-for-automagic-hydration
  {:style/indent 1}
  [dispatch-value & ks]
  (let [[connectable tableable] (dispatch-value-2 dispatch-value)]
    `(do
       ~@(for [k ks]
           `(m/defmethod hydrate/table-for-automagic-hydration* [~connectable ~tableable ~k]
              [~'_ ~'_ ~'_]
              ~tableable)))))

(defmacro deftransforms
  {:style/indent 1}
  [dispatch-value transforms]
  (let [[connectable tableable] (dispatch-value-2 dispatch-value)]
    `(do
       (when-not (isa? ~tableable :bluejdbc/transformed)
         (derive ~tableable :bluejdbc/transformed))
       (m/defmethod transformed/transforms* [~connectable ~tableable]
         [~'&connectable ~'&tableable ~'&options]
         ~transforms))))
