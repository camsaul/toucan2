(ns toucan2.tools.before-update
  (:require
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.select :as select]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(m/defmulti before-update
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod before-update :around :default
  [model row]
  (u/with-debug-result [(list `before-update model row)]
    (next-method model row)))

(defn changes->affected-pk-maps [model reducible-matching-rows changes]
  (reduce
   (fn [changes->pks row]
     (let [row     (merge row changes)
           row     (before-update model row)
           changes (instance/changes row)]
       (cond-> changes->pks
         (seq changes) (update changes (fn [pks]
                                         (conj (set pks) (model/primary-key-values model row)))))))
   {}
   reducible-matching-rows))

(defn apply-before-update-to-matching-rows
  "Fetch the matching rows based on original `parsed-args`; apply [[before-update]] to each. Return a new *sequence* of
  parsed args map that should be used to perform 'replacement' update operations."
  [model {:keys [changes], :as parsed-args}]
  (when-let [changes->pk-maps (not-empty (changes->affected-pk-maps model
                                                                    (select/select-reducible* model parsed-args)
                                                                    changes))]
    (if (= (count changes->pk-maps) 1)
      ;; every row has the same exact changes: we only need to perform a single update, using the original conditions.
      [(assoc parsed-args :changes (first (keys changes->pk-maps)))]
      ;; more than one set of changes: need to do multiple updates.
      (for [[changes pk-maps] changes->pk-maps
            pk-map pk-maps]
        (assoc parsed-args :changes changes, :kv-args pk-map)))))

(m/defmethod op/reducible* :around [::update/update ::before-update]
  [query-type model {::keys [doing-before-update?], :keys [changes], :as parsed-args}]
  (cond
    doing-before-update?
    (next-method query-type model parsed-args)

    (empty? changes)
    (next-method query-type model parsed-args)

    :else
    (u/with-debug-result ["applying before update for %s" model]
      (let [new-args-maps (apply-before-update-to-matching-rows model (assoc parsed-args ::doing-before-update? true))]
        (u/println-debug ["Doing recursive updates with new args maps %s" new-args-maps])
        (eduction
         (mapcat (fn [args-map]
                   (next-method query-type model args-map)))
         new-args-maps)))))

(defmacro define-before-update [model [row-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-update)
     (m/defmethod before-update model#
       [~'&model ~row-binding]
       ~@body)))
