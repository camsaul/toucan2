(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.util :as u]))

;;; this is basically the same as the args for `select` and `delete` but the difference is that it has an additional
;;; optional arg, `:pk`, as the first arg, and one additional optional arg, the `changes` map at the end
(s/def ::default-args
  (s/cat
   :pk        (s/? (complement (some-fn keyword? map?)))
   ;; these are treated as CONDITIONS
   :kv-args   (s/* (s/cat
                    :k keyword?
                    :v any?))
   ;; by default, assuming this resolves to a map query, is treated as a map of conditions.
   :queryable (s/? any?)
   ;; TODO -- we should introduce some sort of `do-with-update-changes` method so it's possible to use named update maps
   ;; here.
   :changes map?))

(m/defmethod query/args-spec [::update :default]
  [_query-type _model]
  ::default-args)

(m/defmethod query/parse-args [::update :default]
  [query-type model unparsed-args]
  (let [{:keys [pk], :as parsed} (next-method query-type model unparsed-args)]
    (cond-> parsed
      pk (-> (dissoc :pk)
             (update :kv-args assoc :toucan/pk pk)))))

(m/defmethod query/build [::update :default clojure.lang.IPersistentMap]
  [query-type model {:keys [kv-args query changes], :as parsed-args}]
  (when (empty? changes)
    (throw (ex-info "Cannot build an update query with no changes."
                    {:query-type query-type, :model model, :parsed-args parsed-args})))
  (let [parsed-args (assoc parsed-args
                           :kv-args (merge kv-args query)
                           :query   {:update [(keyword (model/table-name model))]
                                     :set    changes})]
    (next-method query-type model parsed-args)))

;;;; [[reducible-update]] and [[update!]]

(m/defmulti reducible-update*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod reducible-update* :around :default
  [model parsed-args]
  (u/with-debug-result [(list `reducible-update* model parsed-args)]
    (next-method model parsed-args)))

(defn reduce-reducible-update! [model {:keys [changes], :as parsed-args} rf init]
  (u/with-debug-result "reduce reducible update"
    (if (empty? changes)
      (do
        (u/println-debug "Query has no changes, skipping update")
        (reduce rf init []))
      (let [query (query/build ::update model parsed-args)]
        (try
          (reduce rf init (execute/reducible-query (model/deferred-current-connectable model) model query))
          (catch Throwable e
            (throw (ex-info (format "Error updating rows: %s" (ex-message e))
                            {:model model, :query query}
                            e))))))))

(defrecord ReducibleUpdate [model parsed-args]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce-reducible-update! model parsed-args rf init))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleUpdate model parsed-args)))

(m/defmethod reducible-update* :default
  [model parsed-args]
  (->ReducibleUpdate model parsed-args))

(defn reducible-update
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [::update model unparsed-args]]
      (reducible-update* model parsed-args))))

(defn update!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (u/with-debug-result [(list* `update! modelable unparsed-args)]
    (model/with-model [model modelable]
      (try
        (reduce + 0 (apply reducible-update model unparsed-args))
        (catch Throwable e
          (throw (ex-info (format "Error updating %s: %s" (pr-str model) (ex-message e))
                          {:model model, :unparsed-args unparsed-args}
                          e)))))))

;;;; [[reducible-update-returning-pks]] and [[update-returning-pks!]]

(m/defmulti reducible-update-returning-pks*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod reducible-update-returning-pks* :default
  [model parsed-args]
  (select/return-pks-eduction model (reducible-update* model parsed-args)))

(defn reducible-update-returning-pks
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [::update model unparsed-args]]
      (reducible-update-returning-pks* model parsed-args))))

(defn update-returning-pks!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (u/with-debug-result [(list* `update-returning-pks! modelable unparsed-args)]
    (realize/realize (apply reducible-update-returning-pks modelable unparsed-args))))

;;; TODO -- add `update-returning-instances!`, similar to [[toucan2.insert/insert-returning-instances!]]
