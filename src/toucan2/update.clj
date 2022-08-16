(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]))

;;; this is basically the same as the args for `select` and `delete` but the difference is that it has an additional
;;; optional arg, `:pk`, as the first arg, and one additional optional arg, the `changes` map at the end
(s/def ::default-args
  (s/cat
   :pk        (s/? (complement (some-fn keyword? map?)))
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
  [query-type model {:keys [kv-args query changes], :as args}]
  (let [args (assoc args
                    :kv-args (merge kv-args query)
                    :query   {:update [(keyword (model/table-name model))]
                              :set    changes})]
    (next-method query-type model args)))

(m/defmulti update!*
  "The value of `args` depends on what [[parse-args]] returns for the model."
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod update!* :around :default
  [model parsed-args]
  (u/with-debug-result (pr-str (list 'update!* model parsed-args))
    (next-method model parsed-args)))

(m/defmethod update!* :default
  [model {:keys [changes], :as parsed-args}]
  (if (empty? changes)
    (do
      (u/println-debug "Query has no changes, skipping update")
      0)
    (let [query (query/build ::update model parsed-args)]
      (try
        (execute/query-one (model/deferred-current-connectable model) query)
        (catch Throwable e
          (throw (ex-info (format "Error updating rows: %s" (ex-message e))
                          {:model model, :query query}
                          e)))))))

(defn update!
  "Returns number of rows updated."
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (u/with-debug-result (pr-str (list* 'update! modelable unparsed-args))
    (model/with-model [model modelable]
      (query/with-parsed-args-with-query [parsed-args [::update model unparsed-args]]
        (update!* model parsed-args)))))
