(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]
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
  (let [parsed (next-method query-type model unparsed-args)]
    (cond-> parsed
      (contains? parsed :pk) (-> (dissoc :pk)
                                 (update :kv-args assoc :toucan/pk (:pk parsed))))))

(m/defmethod query/build [::update :default clojure.lang.IPersistentMap]
  [query-type model {:keys [kv-args query changes], :as parsed-args}]
  (when (empty? changes)
    (throw (ex-info "Cannot build an update query with no changes."
                    {:context u/*error-context*, :query-type query-type, :model model, :parsed-args parsed-args})))
  (let [parsed-args (assoc parsed-args
                           :kv-args (merge kv-args query)
                           :query   {:update [(keyword (model/table-name model))]
                                     :set    changes})]
    (next-method query-type model parsed-args)))

(m/defmethod op/reducible-update* [::update :default]
  [query-type model {:keys [changes], :as parsed-args}]
  (if (empty? changes)
    (do
      (u/println-debug "Query has no changes, skipping update")
      nil)
    (next-method query-type model parsed-args)))

(defn reducible-update
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (op/reducible-update* ::update modelable unparsed-args))

(defn update!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (op/returning-update-count! ::update modelable unparsed-args))

(defn reducible-update-returning-pks
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (op/reducible-returning-pks* ::update modelable unparsed-args))

(defn update-returning-pks!
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (op/returning-pks! ::update modelable unparsed-args))

;;; TODO -- add `update-returning-instances!`, similar to [[toucan2.update/insert-returning-instances!]]
