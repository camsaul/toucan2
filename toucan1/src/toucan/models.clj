(ns toucan.models
  "The `defmodel` macro, used to define Toucan models, and
   the `IModel` protocol and default implementations, which implement Toucan model functionality."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [methodical.core :as m]
   [potemkin :as p]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.tools.after-insert :as after-insert]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.after-update :as after-update]
   [toucan2.tools.before-delete :as before-delete]
   [toucan2.tools.before-insert :as before-insert]
   [toucan2.tools.before-select :as before-select]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.tools.hydrate :as hydrate]
   [toucan2.tools.identity-query :as identity-query]
   [toucan2.tools.transformed :as transformed]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(comment default-fields/keep-me)

(defonce ^:private -root-namespace (atom 'models))

(p/import-vars
 [default-fields default-fields define-default-fields])

(defn set-root-namespace!
  "DEPRECATED: In Toucan 2, models do not get resolved from namespaces the way they did in Toucan 1. You generally do not
  need to resolve models, since they are generally just keywords. If you want to introduce special model resolution
  code, you can provide an aux method for [[model/with-model]]."
  [new-root-namespace]
  {:pre [(symbol? new-root-namespace)]}
  (reset! -root-namespace new-root-namespace))

(defn root-namespace
  "DEPRECATED: Toucan 2 retires the concept of a 'root namespace'. See the docstring for [[set-root-namespace!]] for more
  information."
  []
  @-root-namespace)

(defn- model-symb->ns [symb]
  (symbol (str (root-namespace) \. (csk/->kebab-case (name symb)))))

(defn- resolve-model-from-symbol [symb]
  (let [model-ns (model-symb->ns symb)]
    @(try (ns-resolve model-ns symb)
          (catch Throwable _
            (require model-ns)
            (ns-resolve model-ns symb)))))

(defn resolve-model
  "Deprecated: use [[toucan2.model/with-model]] to resolve models instead."
  [model]
  {:post [(isa? % :toucan1/model)]}
  (cond
    (isa? model :toucan1/model) model
    (vector? model)             (resolve-model (first model))
    (symbol? model)             (resolve-model-from-symbol model)
    :else                       (throw (ex-info (str "Invalid model: " (u/safe-pr-str model))
                                                {:model model}))))

(m/defmethod model/do-with-model clojure.lang.Symbol
  [symb f]
  (model/do-with-model (resolve-model-from-symbol symb) f))

(defn model?
  "Is model a legacy-compatibility model defined with [[defmodel]]?

   DEPRECATED: in Toucan 2 anything can be a 'model', so this check no longer makes sense."
  [model]
  (isa? model :toucan1/model))

(defmacro defmodel
  "DEPRECATED: In Toucan 2, you do not generally define models it this fashion. Instead, use [[derive]] to define a model
  as a keyword, and define [[model/table-name]] if needed."
  [model table-name]
  (let [model-keyword (keyword (name (ns-name *ns*)) (name model))]
    `(do
       (derive ~model-keyword :toucan1/model)
       (m/defmethod model/table-name ~model-keyword
         [~'_]
         ~(name table-name))
       (def ~(symbol model) ~model-keyword))))

;;;; Properties

;;; TODO Does this *need* to be a macro?
(defmacro add-property!
  "Define a new 'property' named by namespaced keyword `k`, which these days is really just a helper for defining one or
  more of [[toucan2.tools.before-insert]], [[toucan2.tools.before-update]], or [[toucan2.tools.before-select]] at the
  same time.

  DEPRECATED: Use [[toucan2.tools.before-insert/define-before-insert]],
  [[toucan2.tools.before-update/define-before-update]], and
  [[toucan2.tools.before-select/define-before-select]] directly."
  {:style/indent 1}
  [k & {:keys [insert select], update-fn :update}]
  `(do
     (derive ~k ::property)
     ~(when insert
        `(let [insert-fn# ~insert]
           (before-insert/define-before-insert ~k
             [instance#]
             (insert-fn# instance#))))
     ~(when update-fn
        `(let [update-fn# ~update-fn]
           (before-update/define-before-update ~k
             [instance#]
             (update-fn# instance#))))
     ~(when select
        `(let [select-fn# ~select]
           (before-select/define-before-select ~k
             [instance#]
             (select-fn# instance#))))))

(s/fdef add-property!
  :args (s/cat :key (every-pred keyword? namespace)
               :fns (s/+ (s/cat :fn-type #{:select :insert :update}
                                :fn       any?)))
  :ret  any?)

(defn properties
  "Return a map of properties added by [[add-property!]] for this model."
  [modelable]
  (model/with-model [model modelable]
    (not-empty (into {}
                     (comp (filter (fn [k]
                                     (and (isa? k ::property)
                                          (not= k ::property))))
                           (map (fn [k]
                                  [k true])))
                     (ancestors model)))))

(defn defproperties
  "This is the equivalent of writing an implementation for [[properties]], which was a protocol method in Toucan 1.

  DEPRECATED: you can derive your model directly from properties using [[clojure.core/derive]] instead."
  {:style/indent [:form]}
  [modelable properties-map]
  (model/with-model [model modelable]
    (doseq [[k v] properties-map]
      ;; do we *really* need to enforce this?
      (assert (and (keyword? k)
                   (namespace k)
                   (isa? k ::property))
              (format "Keys in the properties map must be namespaced keywords defined by %s" `add-property!))
      (assert (true? v) "For historical reason, all values in the properties map must be equal to `true`.")
      (derive model k))))

;;;; Types

;;; In Toucan 1 the equivalent of [[toucan2.tools.transformed/deftransforms]] we defined using an [[add-type!]] function
;;; to register them, and models implemented a [[types]] function that mapped column name to keyword type name. e.g.
;;;
;;;    (models/add-type! :lowercase-string
;;;      :in  maybe-lowercase-string
;;;      :out maybe-lowercase-string)
;;;
;;;    (models/defmodel Category :categories
;;;      models/IModel
;;;      (types [_]
;;;        {:name :lowercase-string})
;;;      ...)
;;;
;;; We'll try to replicate something like that below using an atom to store the registered types.

(defmulti ^:private type-fn
  {:arglists '([type-name direction])}
  (fn [type-name direction]
    {:pre [(#{:in :out} direction)]}
    [(keyword type-name) direction]))

(defn- known-type-fns []
  (set (for [[dispatch-value] (methods type-fn)
             :when (sequential? dispatch-value)
             :let [[k _direction] dispatch-value]]
         k)))

;;; Toucan 1 ships with a keyword transform out of the box.

(defn- keyword->qualified-name
  "This is borrowed directly from Toucan 1."
  [k]
  (when k
    (str/replace (str k) #"^:" "")))

(defmethod type-fn :default
  [k _direction]
  (throw (ex-info (format "Unregistered type: %s. Known types: %s"
                          k
                          (pr-str (known-type-fns)))
                  {:k k})))

(defmethod type-fn [:keyword :in]
  [_k _direction]
  keyword->qualified-name)

(defmethod type-fn [:keyword :out]
  [_k _direction]
  keyword)

(defn add-type!
  "DEPRECATED: Toucan 2 does not currently have a transforms registry like Toucan 2 did. Define your transforms with `def`
  and use them directly in a [[toucan2.tools.transformed/deftransforms]]."
  [k & {:keys [in out]}]
  {:pre [(fn? in) (fn? out)]}
  (defmethod type-fn [k :in]  [_k _direction] in)
  (defmethod type-fn [k :out] [_k _direction] out)
  nil)

(defn- resolving-type-fn
  "Return a function that when invoked will invoke the matching function from [[transforms-registry]]"
  [k direction]
  (fn [x]
    ((type-fn k direction) x)))

(defn- type-name->direction->resolving-fn
  "Returns map of `direction => resolving-fn`."
  [k]
  (with-meta {:in  (resolving-type-fn k :in)
              :out (resolving-type-fn k :out)}
             {::type k}))

(defn deftypes
  "e.g.

  ```clj
  (deftypes Venue {:category :keyword})
  ```"
  {:style/indent [:form]}
  [modelable column->k]
  (let [column->direction->fn (into {}
                               (map (fn [[column k]]
                                      [column (type-name->direction->resolving-fn k)]))
                               column->k)]
    (model/with-model [model modelable]
      (transformed/deftransforms model
        column->direction->fn))))

(defn types
  "Get the transforms associated with a model. Returns map of `column name => direction => fn`."
  [modelable]
  (model/with-model [model modelable]
    (into {} (for [[column direction->fn] (transformed/transforms model)
                   :let                   [{type-name ::type} (meta direction->fn)]
                   :when                  type-name]
               [column type-name]))))

;;;; Replicating the rest of the old `IModel` protocol

(defn primary-key
  "DEPRECATED: use [[toucan2.model/primary-keys]] instead."
  [modelable]
  (let [modelable (resolve-model modelable)]
    (model/with-model [model modelable]
      (first (model/primary-keys model)))))

(defn define-primary-key [modelable pk]
  (model/with-model [model modelable]
    (m/defmethod model/primary-keys model
      [_model]
      [pk])))

;;; Toucan 1 has no `pre-select`

(defn do-post-select
  "Do [[toucan2.tools.after-select]] stuff for row map `object` using methods for `modelable`."
  [modelable row-map]
  {:pre [(map? row-map)]}
  (model/with-model [model modelable]
    (select/select-one model (identity-query/identity-query [row-map]))))

(defn post-select
  "Do [[toucan2.tools.after-select]] stuff for an `instance`."
  [instance]
  {:pre [(instance/instance? instance)]}
  (do-post-select (protocols/model instance) instance))

(defn do-pre-insert
  "Do the [[toucan2.tools.before-insert]] stuff for a `row-map` using the methods for `modelable`."
  [modelable row-map]
  {:pre [(map? row-map)]}
  (model/with-model [model modelable]
    (-> (tools.compile/build
          (insert/insert! model row-map))
        :values
        first)))

(defn pre-insert
  "Do the [[toucan2.tools.before-insert]] stuff for an `instance`."
  [instance]
  {:pre [(instance/instance? instance)]}
  (do-pre-insert (protocols/model instance) instance))

(derive ::post-insert ::insert/insert)

(m/defmethod op/reducible-returning-instances* [::post-insert :default]
  [_query-type _model parsed-args]
  (:instances parsed-args))

(defn post-insert
  "Do [[toucan2.tools.before-update]] stuff for an `instance`."
  [instance]
  {:pre [(instance/instance? instance)]}
  (let [model (protocols/model instance)]
    (realize/reduce-first
     (eduction
      ;; HACK -- we shouldn't need to look into [[transformed]] internals, but it's broken so we have to.
      (transformed/transform-result-rows-transducer model)
      (op/reducible-returning-instances* ::post-insert
                                         model
                                         {:instances [instance]})))))

;;; TODO -- this is an extremely wack way to implement this, because it only applies changes from
;;; [[toucan2.tools.before-update]] and [[toucan2.tools.transformed]]. Maybe that's ok because that is all that was used
;;; for Toucan 1. But we shouldn't have to poke into their internals to get the transforms -- we need a general way to
;;; do this
(defn do-pre-update
  "Do [[toucan2.tools.before-update]] stuff for a `changes-map` using the methods for `modelable`."
  [modelable changes-map]
  {:pre [(map? changes-map)]}
  ;; mega HACK
  (model/with-model [model modelable]
    (as-> changes-map changes-map
      ;; make sure a method exists before calling it so it doesn't error
      (cond->> changes-map
        (m/applicable-primary-method before-update/before-update model)
        (before-update/before-update model))
      ;; apply the transformed changes.
      (:kv-args (transformed/apply-in-transforms model {:kv-args changes-map})))))

(defn pre-update
  "Do [[toucan2.tools.before-update]] stuff for an `instance`."
  [changes-instance]
  {:pre [(instance/instance? changes-instance)]}
  (do-pre-update (protocols/model changes-instance) changes-instance))

(derive ::post-update ::update/update)

(m/defmethod op/reducible-returning-instances* [::post-update :default]
  [_query-type _model parsed-args]
  (:instances parsed-args))

(defn post-update
  "Do [[toucan2.tools.after-update]] stuff for an `instance`."
  [instance]
  {:pre [(instance/instance? instance)]}
  (let [model (protocols/model instance)]
    (realize/reduce-first
     (eduction
      ;; HACK -- we shouldn't need to look into [[transformed]] internals, but it's broken so we have to.
      (transformed/transform-result-rows-transducer model)
      (op/reducible-returning-instances* ::post-update
                                         model
                                         {:instances [instance]})))))

(defn pre-delete
  "Do [[toucan2.tools.before-delete]] stuff for an `instance`."
  [instance]
  {:pre [(instance/instance? instance)]}
  (let [model (protocols/model instance)]
    ;; mega HACK
    (as-> instance instance
      ;; make sure a method exists before calling it so it doesn't error
      (cond->> instance
        (m/applicable-primary-method before-delete/before-delete model)
        (before-delete/before-delete model))
      ;; apply the transformed changes.
      (:kv-args (transformed/apply-in-transforms model {:kv-args instance})))))

(defn hydration-keys
  "Get the keys that automagically hydrate to a model.

  ```clj
  (hydration-keys Venue) => [:venue :location]
  ```"
  [modelable]
  (model/with-model [model modelable]
    ;; programatically try all the hydration methods with `[:default <k>]` dispatch values and see which of them returns
    ;; our `model` when invoked. This is a totally wacky way of doing this. But it lets us introspect things even if
    ;; they weren't defined with [[define-hydration-keys]].
    (for [[dispatch-value f] (m/primary-methods hydrate/model-for-automagic-hydration)
          :when              (sequential? dispatch-value)
          :let               [[original-model k] dispatch-value]
          :when              (= original-model :default)
          :let               [next-method (constantly nil)
                              hydrating-model (f next-method nil k)]
          :when              (isa? model hydrating-model)]
      k)))

(defn define-hydration-keys
  "Define the keys that we should automagically hydrate with instances of this model.

  ```clj
  (define-hydration-keys Venue [:venue :location])
  ```

  DEPRECATED: prefer using [[toucan2.tools.model-for-automagic-hydration]] directly instead."
  {:style/indent [:form]}
  [modelable ks]
  {:pre [(sequential? ks) (every? keyword? ks)]}
  (model/with-model [model modelable]
    (doseq [k ks]
      (m/defmethod hydrate/model-for-automagic-hydration [:default k]
        [_original-model _k]
        model))))

;;;; these let you use method maps passed to [[extend]] for the old `IModel` protocol to implement Toucan 2 multimethods.

(defmulti ^:private define-method-with-IModel-method
  {:arglists '([method-name model f])}
  u/dispatch-on-first-arg)

(defmethod define-method-with-IModel-method :default-fields
  [_k model f]
  (define-default-fields model
    (f model)))

(defmethod define-method-with-IModel-method :hydration-keys
  [_k model f]
  (define-hydration-keys model (f model)))

(defmethod define-method-with-IModel-method :post-insert
  [_k model f]
  (after-insert/define-after-insert model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :post-select
  [_k model f]
  (after-select/define-after-select model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :post-update
  [_k model f]
  (after-update/define-after-update model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :pre-delete
  [_k model f]
  (before-delete/define-before-delete model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :pre-insert
  [_k model f]
  (before-insert/define-before-insert model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :pre-update
  [_k model f]
  (before-update/define-before-update model
    [row]
    (f row)))

(defmethod define-method-with-IModel-method :primary-key
  [_k model f]
  (define-primary-key model (f model)))

(defmethod define-method-with-IModel-method :properties
  [_k model f]
  (defproperties model (f model)))

(defmethod define-method-with-IModel-method :types
  [_k model f]
  (deftypes model (f model)))

(defn define-methods-with-IModel-method-map
  {:style/indent [:form]}
  [model method-map]
  (doseq [[k f] method-map]
    (define-method-with-IModel-method k model f)))
