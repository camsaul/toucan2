(ns toucan2.tools.before-update
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(derive ::select-for-before-update :toucan.query-type/select.instances.from-update)

(m/defmulti before-update
  {:arglists            '([model₁ row])
   :defmethod-arities   #{2}
   ;; work around https://github.com/camsaul/methodical/issues/142
   :dispatch-value-spec (s/nonconforming
                         (s/or
                          :default (partial = :default)
                          :model   (complement sequential?)))}
  u/dispatch-on-first-arg)

(m/defmethod before-update :around :default
  [model row]
  (assert (map? row) (format "Expected a map row, got ^%s %s" (some-> row class .getCanonicalName) (pr-str row)))
  (log/debugf :compile "before-update %s %s" model row)
  (let [result (next-method model row)]
    (assert (map? result) (format "%s for %s should return a map, got %s" `before-update model (pr-str result)))
    (log/debugf :compile "[before-update] => %s" result)
    result))

(defn- changes->affected-pk-maps-rf [model changes]
  (assert (map? changes) (format "Expected changes to be a map, got %s" (pr-str changes)))
  (fn
    ([] {})
    ([m]
     (assert (map? m) (format "changes->affected-pk-maps-rf should have returned a map, got %s" (pr-str m)))
     m)
    ([changes->pks row]
     (assert (map? changes->pks))
     (assert (map? row) (format "%s expected a map row, got %s" `changes->affected-pk-maps (pr-str row)))
     ;; After going back and forth on this I've concluded that it's probably best to just realize the entire row here.
     ;; There are a lot of situations where we don't need to do this, but it means we have to step on eggshells
     ;; everywhere else in order to make things work nicely. Maybe we can revisit this in the future.
     (let [row         (realize/realize row)
           row         (merge row changes)
           row         (before-update model row)
           ;; if the `before-update` method returned a plain map then consider that to be the changes.
           ;; `protocols/changes` will return `nil` for non-instances. TODO -- does that behavior make sense? Clearly,
           ;; it's easy to use wrong -- it took me hours to figure out why something was working and that I needed to
           ;; make this change :sad:
           row-changes (if (instance/instance? row)
                         (protocols/changes row)
                         row)]
       (log/tracef :compile "The following values have changed: %s" changes)
       (cond-> changes->pks
         (seq row-changes) (update row-changes (fn [pks]
                                                 (conj (set pks) (model/primary-key-values-map model row)))))))))

(defn- fetch-changes->pk-maps [model {:keys [changes], :as parsed-args}]
  (not-empty
   (pipeline/transduce-with-model
    (changes->affected-pk-maps-rf model changes)
    ::select-for-before-update
    model
    parsed-args)))

;;; TODO -- this is sort of problematic since it breaks [[toucan2.tools.compile]]
(defn- apply-before-update-to-matching-rows
  "Fetch the matching rows based on original `parsed-args`; apply [[before-update]] to each. Return a new *sequence* of
  parsed args maps that should be used to perform 'replacement' update operations."
  [model {:keys [changes], :as parsed-args}]
  (u/try-with-error-context ["apply before-update to matching rows" {::model model, ::changes changes}]
    (log/debugf :compile "apply before-update to matching rows for %s" model)
    (when-let [changes->pk-maps (fetch-changes->pk-maps model parsed-args)]
      (log/tracef :compile "changes->pk-maps = %s" changes->pk-maps)
      (if (= (count changes->pk-maps) 1)
        ;; every row has the same exact changes: we only need to perform a single update, using the original
        ;; conditions.
        [(assoc parsed-args :changes (first (keys changes->pk-maps)))]
        ;; more than one set of changes: need to do multiple updates.
        (for [[changes pk-maps] changes->pk-maps
              pk-map            pk-maps]
          (assoc parsed-args :changes changes, :kv-args pk-map))))))

;;; TODO -- why is this an around method? All it does is make things confusing when there's ambiguity
(m/defmethod pipeline/transduce-with-model :around [#_query-type :toucan.query-type/update.* #_model ::before-update]
  "Apply [[toucan2.tools.before-update/before-update]] to matching rows. If multiple versions of `:changes` are produced
  as a result, recursively does an update for each version."
  [rf query-type model {::keys [doing-before-update?], :keys [changes], :as parsed-args}]
  (cond
    doing-before-update?
    (next-method rf query-type model parsed-args)

    (empty? changes)
    (next-method rf query-type model parsed-args)

    :else
    (let [new-args-maps (apply-before-update-to-matching-rows model (assoc parsed-args ::doing-before-update? true))]
      (log/debugf :execute "Doing recursive updates with new args maps %s" new-args-maps)
      (conn/with-transaction [_conn nil {:nested-transaction-rule :ignore}]
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
