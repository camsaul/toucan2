(ns toucan.models
  "The `defmodel` macro, used to define Toucan models, and
   the `IModel` protocol and default implementations, which implement Toucan model functionality."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.select :as select]
   [toucan2.tools.before-insert :as before-insert]
   [toucan2.tools.before-select :as before-select]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.identity-query :as identity-query]
   [toucan2.util :as u]))

(defonce ^:private -root-namespace (atom 'models))

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

(def ^:dynamic ^:deprecated *disable-db-logging*
  "DEPRECATED: Toucan 2 does not currently have 'DB logging' to enable or disable, and even if it did, it's unlikely that
  we would have a with a dynamic var for toggling it. Instead. we'll probably use [[clojure.tools.logging]]. This is
  here mostly to minimize the number of changes you need to make to existing code. Binding it has no effect whatsoever."
  false)

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

(defn- qualify-property-keyword [k]
  (if (namespace k)
    k
    (keyword "toucan1.models.properties" (name k))))

(defmacro add-property!
  {:style/indent 1}
  [k & {:keys [insert select], update-fn :update}]
  (let [property-keyword (qualify-property-keyword k)]
    `(do
       ~(when insert
          `(let [insert-fn# ~insert]
             (before-insert/define-before-insert ~property-keyword
               [instance#]
               (insert-fn# instance#))))
       ~(when update-fn
          `(let [update-fn# ~update-fn]
             (before-update/define-before-update ~property-keyword
               [instance#]
               (update-fn# instance#))))
       ~(when select
          `(let [select-fn# ~select]
             (before-select/define-before-select ~property-keyword
               [instance#]
               (select-fn# instance#)))))))

(s/fdef add-property!
  :args (s/cat :key keyword?
               :fns (s/+ (s/cat :fn-type #{:select :insert :update}
                                :fn       any?)))
  :ret  any?)

(defn primary-key
  "DEPRECATED: use [[toucan2.model/primary-keys]] instead."
  [modelable]
  (let [modelable (resolve-model modelable)]
    (model/with-model [model modelable]
      (first (model/primary-keys model)))))

#_(defn do-pre-insert
  [model obj]
  ;; TODO
  obj
  )

#_(defn do-pre-update
  [model obj]
  ;; TODO
  obj
  )

(defn do-post-select [modelable object]
  (select/select-one modelable (identity-query/identity-query [object])))

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
