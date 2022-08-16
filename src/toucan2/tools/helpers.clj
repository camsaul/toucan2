(ns toucan2.tools.helpers
  (:require
   [methodical.core :as m]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.tools.transformed :as transformed]
   [toucan2.update :as update]
   [toucan2.util :as u]
   [toucan2.realize :as realize]
   [toucan2.delete :as delete]
   [toucan2.connection :as conn]))

(defn maybe-derive
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))

;;;; [[define-before-select]], [[define-after-select-reducible]], [[define-after-select-each]]

(defn do-before-select [model thunk]
  (u/with-debug-result ["%s %s" `define-before-select model]
    (try
      (thunk)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `define-before-select (pr-str model) (ex-message e))
                        {:model model}
                        e))))))

(defmacro define-before-select
  {:style/indent :defn, :arglists '([model [args-binding] & body]
                                    [[model query-class] [args-binding] & body])}
  [dispatch-value [args-binding] & body]
  (let [[model query-class] (if (vector? dispatch-value)
                              dispatch-value
                              [dispatch-value clojure.lang.IPersistentMap])]
    `(m/defmethod query/build :before [::select/select ~model ~query-class]
       [~'&query-type ~'&model ~args-binding]
       (do-before-select ~'&model (^:once fn* [] ~@body)))))

(defmacro define-after-select-reducible
  {:style/indent :defn}
  [model [reducible-query-binding] & body]
  `(m/defmethod select/select-reducible* :after ~model
     [~'&model ~reducible-query-binding]
     ~@body))

(defn do-after-select-each [model reducible-query f]
  (eduction (map (fn [instance]
                   (u/with-debug-result [(list `define-after-select-each model instance)]
                     (try
                       (f instance)
                       (catch Throwable e
                         (throw (ex-info (format "Error in %s for %s: %s"
                                                 `define-after-select-each (pr-str model) (ex-message e))
                                         {:model model, :instance instance}
                                         e)))))))
            reducible-query))

(defmacro define-after-select-each
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(define-after-select-reducible ~model [reducible-query#]
     (do-after-select-each ~'&model reducible-query# (fn [~instance-binding] ~@body))))

;;;; [[define-default-fields]]

(m/defmulti default-fields
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod query/build :before [::select/select ::default-fields clojure.lang.IPersistentMap]
  [_query-type model args]
  (update args :query (fn [query]
                        (merge {:select (default-fields model)}
                               query))))

(defmacro define-default-fields [model & body]
  `(let [model# ~model]
     (maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))

;;;; [[define-before-update]], [[define-after-update]]

(m/defmulti before-update
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod before-update :around :default
  [model row]
  (u/with-debug-result [(list `before-update model row)]
    (next-method model row)))

(defn before-update-changes->affected-pk-maps [model reducible-matching-rows changes]
  (reduce
   (fn [changes->pks row]
     (let [row (merge row changes)
           row (before-update model row)]
       (update changes->pks
               (instance/changes row)
               (fn [pks]
                 (conj (set pks) (model/primary-key-values model row))))))
   {}
   reducible-matching-rows))

(defn do-before-update-with-args [model {:keys [changes], :as parsed-args}]
  (let [reducible-matching-rows   (select/select-reducible* model parsed-args)
        changes->affected-pk-maps (before-update-changes->affected-pk-maps model reducible-matching-rows changes)]
    (if (= (count (keys changes->affected-pk-maps)) 1)
      ;; every row has the same exact changes: we only need to perform a single update, using the original conditions.
      (update/update!* model (assoc parsed-args :changes (first (keys changes->affected-pk-maps))))
      ;; More than one set of changes: update each row individually.
      ;;
      ;; TODO -- we should also batch these together as much as possible. If we update 100 rows with two possible change
      ;; sets then we should perform 2 updates, not 100.
      (reduce
       (fn [update-count [changes affected-pk-maps]]
         (reduce
          (fn [update-count pk-map]
            (+ update-count (update/update!* model (assoc parsed-args
                                                          :changes changes
                                                          :kv-args pk-map))))
          update-count
          affected-pk-maps))
       0
       changes->affected-pk-maps))))

(def ^:dynamic *doing-before-update?* false)

(m/defmethod update/update!* ::before-update
  [model {:keys [changes], :as parsed-args}]
  (cond
    ;; if there are no changes we don't need to do anything -- just no-op.
    (zero? (count changes))
    0

    ;; if we're already doing special before-update stuff then don't do the special stuff on top of that again.
    *doing-before-update?*
    (next-method model parsed-args)

    :else
    (binding [*doing-before-update?* true]
      (do-before-update-with-args model parsed-args))))

(defmacro define-before-update [model [row-binding] & body]
  `(let [model# ~model]
     (maybe-derive model# ::before-update)
     (m/defmethod before-update model#
       [~'&model ~row-binding]
       ~@body)))

(m/defmulti after-update
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod after-update :around :default
  [model instance]
  (u/with-debug-result [(list `after-update model instance)]
    (next-method model instance)))

(defn do-after-update [model parsed-args]
  (println "parsed-args:" parsed-args) ; NOCOMMIT
  (println "ROWS" (realize/realize (select/select-reducible* model parsed-args)))
  (try
    (reduce
     (fn [_ instance]
       (println "instance:" instance)   ; NOCOMMIT
       (after-update model instance)
       nil)
     nil
     (select/select-reducible* model parsed-args))
    (catch Throwable e
      (throw (ex-info (format "Error in after-update for %s: %s" (pr-str model) (ex-message e))
                      {:model model, :args parsed-args}
                      e)))))

(def ^:dynamic *doing-after-update?* false)

(defn reducible-after-update [model affected-pks]
  (eduction
   (map (fn [row]
          (after-update model row)))
   (select/select-reducible-with-pks model affected-pks)))

(m/defmethod update/update!* ::after-update
  [model parsed-args]
  (if *doing-after-update?*
    (next-method model parsed-args)
    (binding [*doing-after-update?* true]
      (let [affected-pks (update/update-returning-pks!* model parsed-args)]
        (transduce
         (map (constantly 1))
         (completing +)
         0
         (reducible-after-update model affected-pks))))))

(m/defmethod update/update-returning-pks!* ::after-update
  [model parsed-args]
  (if *doing-after-update?*
    (next-method model parsed-args)
    (binding [*doing-after-update?* true]
      (let [affected-pks (next-method model parsed-args)]
        (reduce
         (constantly nil)
         nil
         (reducible-after-update model affected-pks))
        affected-pks))))

(defmacro define-after-update
  {:style/indent :defn}
  [model [result-binding] & body]
  `(let [model# ~model]
     (maybe-derive model# ::after-update)
     (m/defmethod after-update model#
       [~'&model ~result-binding]
       ~@body)))

;;;; [[define-before-insert]], [[define-after-insert]]

(m/defmulti before-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(defn do-before-insert-to-rows [rows model]
  (mapv
   (fn [row]
     (try
       (u/with-debug-result ["Do before-insert for %s" model]
         (before-insert model row))
       (catch Throwable e
         (throw (ex-info (format "Error in before-insert for %s: %s" (pr-str model) (ex-message e))
                         {:model model, :row row}
                         e)))))
   rows))

(m/defmethod insert/insert!* :before ::before-insert
  [model parsed-args]
  (update parsed-args :rows do-before-insert-to-rows model))

(defmacro define-before-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (derive model# ::before-insert)
     (m/defmethod before-insert model#
       [~'&model ~instance-binding]
       ~@body)))

(m/defmulti after-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod after-insert :around :default
  [model row]
  (u/with-debug-result [(list `after-insert model row)]
    (try
      (next-method model row)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `after-insert (pr-str model) (ex-message e))
                        {:model model, :row row}
                        e))))))

(def ^:dynamic *doing-after-insert?* false)

(m/defmethod insert/insert!* ::after-insert
  [model parsed-args]
  (if *doing-after-insert?*
    (next-method model parsed-args)
    (binding [*doing-after-insert?* true]
      (let [rows (insert/insert-returning-instances!* model parsed-args)]
        (doseq [row rows]
          (after-insert model row))
        (count rows)))))

(m/defmethod insert/insert-returning-keys!* ::after-insert
  [model parsed-args]
  (if *doing-after-insert?*
    (next-method model parsed-args)
    (binding [*doing-after-insert?* true]
      (let [row-pks (next-method model parsed-args)
            rows    (insert/select-rows-with-pks model parsed-args row-pks)]
        (doseq [row rows]
          (after-insert model row))
        row-pks))))

(m/defmethod insert/insert-returning-instances!* ::after-insert
  [model parsed-args]
  (if *doing-after-insert?*
    (next-method model parsed-args)
    (binding [*doing-after-insert?* true]
      (mapv (partial after-insert model)
            (next-method model parsed-args)))))

(defmacro define-after-insert {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (maybe-derive model# ::after-insert)
     (m/defmethod after-insert model#
       [~'&model ~instance-binding]
       ~@body)))

;;;; [[define-before-delete]], [[define-after-delete]]

(m/defmulti before-delete
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (u/with-debug-result [(list `before-delete model instance)]
    (next-method model instance)))

(m/defmethod delete/delete!* :around ::before-delete
  [model parsed-args]
  (conn/with-transaction [_conn (model/deferred-current-connectable model)]
    (transduce
     (map (fn [row]
            (before-delete model row)))
     (constantly nil)
     nil
     (select/select-reducible* model parsed-args))
    (next-method model parsed-args)))

(defmacro define-before-delete
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (maybe-derive model# ::before-delete)
     (m/defmethod before-delete model#
       [~'&model ~instance-binding]
       ~@body)))

;; TODO
#_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
    (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
      [~'&~'&model _ ~'&options]
      ~@body))

;;;; [[define-keys-for-automagic-hydration]]

#_(defmacro define-keys-for-automagic-hydration
  {:style/indent 1}
  [dispatch-value & ks]
  (let [[model] (dispatch-value-2 dispatch-value)]
    `(do
       ~@(for [k ks]
           `(m/defmethod hydrate/table-for-automagic-hydration* [~~model ~k]
              [~'_ ~'_ ~'_]
              ~model)))))

;;;; [[deftransforms]]

(defmacro deftransforms
  "`transforms` should be a map of

    {column-name {:in <fn>, :out <fn>}}"
  {:style/indent 1}
  [model transforms]
  `(let [model# ~model]
     (maybe-derive model# ::transformed/transformed)
     (m/defmethod transformed/transforms* model#
       [~'&model]
       ~transforms)))
