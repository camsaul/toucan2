(ns toucan2.tools.before-update
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

(m/defmulti before-update
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod before-update :around :default
  [model row]
  (u/with-debug-result (list `before-update model row)
    (doto (next-method model row)
      ((fn [result]
         (assert (map? result) (format "%s for %s should return a map, got %s"
                                       `before-update
                                       model
                                       (u/safe-pr-str result))))))))

(defn- changes->affected-pk-maps-rf [model changes]
  {:pre [(map? changes)]}
  (fn
    ([] {})
    ([m]
     (assert (map? m) (format "changes->affected-pk-maps-rf should have returned a map, got %s" (u/safe-pr-str m)))
     m)
    ([changes->pks row]
     (assert (map? changes->pks))
     (assert (map? row) (format "%s expected a map row, got %s" `changes->affected-pk-maps (u/safe-pr-str row)))
     (let [row     (merge row changes)
           row     (before-update model row)
           changes (protocols/changes row)]
       (cond-> changes->pks
         (seq changes) (update changes (fn [pks]
                                         (conj (set pks) (model/primary-key-values model row)))))))))

;;; TODO -- this is sort of problematic since it breaks [[toucan2.tools.compile]]
(defn- apply-before-update-to-matching-rows
  "Fetch the matching rows based on original `parsed-args`; apply [[before-update]] to each. Return a new *sequence* of
  parsed args map that should be used to perform 'replacement' update operations."
  [model {:keys [changes], :as parsed-args}]
  (u/try-with-error-context ["apply before-update to matching rows" {::model model, ::changes changes}]
    (u/with-debug-result ["%s for %s" `apply-before-update-to-matching-rows model]
      (when-let [changes->pk-maps (not-empty (let [rf (changes->affected-pk-maps-rf model changes)]
                                               (pipeline/transduce-with-model
                                                rf
                                                :toucan.query-type/select.instances
                                                model
                                                parsed-args)))]
        (u/println-debug ["changes->pk-maps = %s" changes->pk-maps])
        (if (= (count changes->pk-maps) 1)
          ;; every row has the same exact changes: we only need to perform a single update, using the original conditions.
          [(assoc parsed-args :changes (first (keys changes->pk-maps)))]
          ;; more than one set of changes: need to do multiple updates.
          (for [[changes pk-maps] changes->pk-maps
                pk-map            pk-maps]
            (assoc parsed-args :changes changes, :kv-args pk-map)))))))

(m/defmethod pipeline/transduce-with-model :around [:toucan.query-type/update.* ::before-update]
  [rf query-type model {::keys [doing-before-update?], :keys [changes], :as parsed-args}]
  (cond
    doing-before-update?
    (next-method rf query-type model parsed-args)

    (empty? changes)
    (next-method rf query-type model parsed-args)

    :else
    (let [new-args-maps (apply-before-update-to-matching-rows model (assoc parsed-args ::doing-before-update? true))]
      (u/println-debug ["Doing recursive updates with new args maps %s" new-args-maps])
      (conn/with-transaction [_conn
                              (or conn/*current-connectable*
                                  (model/default-connectable model))
                              {:nested-transaction-rule :ignore}]
        (transduce
         (map (fn [args-map]
                (next-method rf query-type model args-map)))
         rf
         new-args-maps)))))

(defmacro define-before-update [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-update)
     (m/defmethod before-update model#
       [~'&model ~instance-binding]
       (cond->> (do ~@body)
         ~'next-method
         (~'next-method ~'&model)))))

(s/fdef define-before-update
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
