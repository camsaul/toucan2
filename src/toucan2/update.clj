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
(s/def ::args
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

(defn parse-update-args
  [query-type unparsed-args]
  (let [parsed (query/parse-args query-type ::args unparsed-args)]
    (cond-> parsed
      (contains? parsed :pk) (-> (dissoc :pk)
                                 (update :kv-args assoc :toucan/pk (:pk parsed))))))

(m/defmethod pipeline/transduce-unparsed :toucan.query-type/update.*
  [rf query-type unparsed-args]
  (let [parsed-args (parse-update-args query-type unparsed-args)]
    (pipeline/transduce-parsed-args rf query-type parsed-args)))

(m/defmethod pipeline/transduce-resolved-query [#_query-type :toucan.query-type/update.*
                                                #_model      :default
                                                #_query      clojure.lang.IPersistentMap]
  [rf query-type model {:keys [kv-args changes], :as parsed-args} query]
  (let [parsed-args (assoc parsed-args :kv-args (merge kv-args query))
        built-query       {:update (query/honeysql-table-and-alias model)
                           :set    changes}]
    ;; `:changes` are added to `parsed-args` so we can get the no-op behavior in the default method.
    (next-method rf query-type model (assoc parsed-args :changes changes) built-query)))

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
