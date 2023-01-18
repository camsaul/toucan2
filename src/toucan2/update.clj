(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

;;; this is basically the same as the args for `select` and `delete` but the difference is that it has an additional
;;; optional arg, `:pk`, as the second arg, and one additional optional arg, the `changes` map at the end
(s/def ::args
  (s/cat
   :connectable ::query/default-args.connectable
   :modelable   ::query/default-args.modelable
   :pk          (s/? (complement (some-fn keyword? map?)))
   ;; these are treated as CONDITIONS
   :kv-args     ::query/default-args.kv-args
   ;; by default, assuming this resolves to a map query, is treated as a map of conditions.
   :queryable   ::query/default-args.queryable
   ;; TODO -- we should introduce some sort of `do-with-update-changes` method so it's possible to use named update maps
   ;; here.
   :changes     map?))

(m/defmethod query/parse-args :toucan.query-type/update.*
  [query-type unparsed-args]
  (let [parsed (query/parse-args-with-spec query-type ::args unparsed-args)]
    (cond-> parsed
      (contains? parsed :pk) (-> (dissoc :pk)
                                 (update :kv-args assoc :toucan/pk (:pk parsed))))))

;;;; Code for building Honey SQL for UPDATE lives in [[toucan2.map-backend.honeysql2]]

(m/defmethod pipeline/build [#_query-type :toucan.query-type/update.*
                             #_model      :default
                             #_query      :default]
  [query-type model {:keys [changes], :as parsed-args} resolved-query]
  (if (empty? changes)
    (do
      (log/debugf :compile "Query has no changes, skipping update")
      ::pipeline/no-op)
    (next-method query-type model parsed-args resolved-query)))

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
