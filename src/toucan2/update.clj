(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.util :as u]))

;;; this is basically the same as the args for `select` and `delete` but the difference is that it has an additional
;;; optional arg, `:pk`, as the second arg, and one additional optional arg, the `changes` map at the end
(s/def ::default-args
  (s/cat
   :modelable ::query/default-args.modelable
   :pk        (s/? (complement (some-fn keyword? map?)))
   ;; these are treated as CONDITIONS
   :kv-args   ::query/default-args.kv-args
   ;; by default, assuming this resolves to a map query, is treated as a map of conditions.
   :queryable ::query/default-args.queryable
   ;; TODO -- we should introduce some sort of `do-with-update-changes` method so it's possible to use named update maps
   ;; here.
   :changes map?))

(m/defmethod query/args-spec :toucan.query-type/update.*
  [_query-type]
  ::default-args)

(defn parse-update-args
  [query-type unparsed-args]
  (let [parsed (query/parse-args query-type unparsed-args)]
    (cond-> parsed
      (contains? parsed :pk) (-> (dissoc :pk)
                                 (update :kv-args assoc :toucan/pk (:pk parsed))))))

(m/defmethod pipeline/transduce-unparsed :toucan.query-type/update.*
  [rf query-type unparsed-args]
  (let [parsed-args (parse-update-args query-type unparsed-args)]
    (pipeline/transduce-parsed-args rf query-type parsed-args)))

(m/defmethod query/build [:toucan.query-type/update.* :default clojure.lang.IPersistentMap]
  [query-type model {:keys [kv-args query changes], :as parsed-args}]
  (when (empty? changes)
    (throw (ex-info "Cannot build an update query with no changes."
                    {:query-type query-type, :model model, :parsed-args parsed-args})))
  (let [parsed-args (assoc parsed-args
                           :kv-args (merge kv-args query)
                           :query   {:update (query/honeysql-table-and-alias model)
                                     :set    changes})]
    (next-method query-type model parsed-args)))

(m/defmethod pipeline/transduce-resolved-query [#_query-type :toucan.query-type/update.*
                                                #_model      :default
                                                #_query      :default]
  [rf query-type model {:keys [changes], :as parsed-args} resolved-query]
  (if (empty? changes)
    (do
      (u/println-debug "Query has no changes, skipping update")
      ;; TODO -- not sure this is the right thing to do
      (rf (rf)))
    (next-method rf query-type model parsed-args resolved-query)))

(defn reducible-update
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/update.update-count unparsed-args))

(defn update!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/update.update-count unparsed-args))

(defn reducible-update-returning-pks
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/update.pks unparsed-args))

(defn update-returning-pks!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/update.pks unparsed-args))

;;; TODO -- add `update-returning-instances!`, similar to [[toucan2.update/insert-returning-instances!]]
