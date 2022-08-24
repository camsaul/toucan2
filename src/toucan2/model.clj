(ns toucan2.model
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

(m/defmulti do-with-model
  {:arglists '([modelable f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-model :default
  [model f]
  (f model))

(m/defmethod do-with-model :around :default
  [modelable f]
  (next-method modelable
               (^:once fn* [model]
                (when-not (= model modelable)
                  (u/println-debug ["Resolved modelable %s => model %s" modelable model]))
                (u/try-with-error-context ["with resolved model" {::modelable modelable, ::model model}]
                  (f model)))))

(defmacro with-model [[model-binding modelable] & body]
  `(do-with-model ~modelable (^:once fn* [~model-binding] ~@body)))

(m/defmulti default-connectable
  "The default connectable that should be used when executing queries for `model` if
  no [[toucan2.connection/*current-connectable*]] is currently bound. By default, this just returns the global default
  connectable, `:default`, but you can tell Toucan to use a different default connectable for a model by implementing
  this method."
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod default-connectable :default
  [_model]
  ;; TODO -- or should this return `nil`, so we can fall thru to something else (presumably `:default` anyway)?
  :default)

(defn- current-connectable [model]
  (u/with-debug-result ["Realizing deferred current connectable for model %s." model]
    (u/println-debug ["%s is %s" `conn/*current-connectable* conn/*current-connectable*])
    (or conn/*current-connectable*
        (do
          (u/println-debug ["Using %s for model %s" `default-connectable model])
          (default-connectable model)))))

(defrecord ^:no-doc DeferredCurrentConnectable [model]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `deferred-current-connectable model))

  protocols/IModel
  (model [_this]
    model)

  protocols/IWithModel
  (with-model [_this new-model]
    (DeferredCurrentConnectable. new-model)))

(m/defmethod conn/do-with-connection DeferredCurrentConnectable
  [{:keys [model]} f]
  (let [connectable (current-connectable model)]
    (assert (not (instance? DeferredCurrentConnectable connectable))
            (format "%s returned another %s" `current-connectable `DeferredCurrentConnectable))
    (conn/do-with-connection connectable f)))

(defn deferred-current-connectable [model]
  (->DeferredCurrentConnectable model))

(m/defmulti table-name
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod table-name :default
  [model]
  (if (instance? clojure.lang.Named model)
    ((m/effective-method table-name clojure.lang.Named) model)
    (throw (ex-info (format "Invalid model %s: don't know how to get its table name." (u/safe-pr-str model))
                    {:model model}))))

(m/defmethod table-name clojure.lang.Named
  [model]
  (name model))

(m/defmethod table-name String
  [table]
  table)

(m/defmulti primary-keys
  "Return a sequence of the primary key columns names, as keywords, for a model. The default primary keys for a model are
  `[:id]`; implement this method if your model has different primary keys.

  ```clj
  ;; tell Toucan that :model/bird has a composite primary key consisting of the columns :id and :name
  (m/defmethod primary-keys :model/bird
    [_model]
    [:id :name])
  ```

  If an implementation returns a single keyword, the default `:around` method will automatically wrap it in a vector. It
  also validates that the ultimate result is a sequence of keywords, so it is safe to assume that calls to this will
  always return a sequence of keywords."
  {:arglists '([model])}
  u/dispatch-on-first-arg)

;;; if the PK comes back unwrapped, wrap it.
(m/defmethod primary-keys :around :default
  [model]
  (let [pk-or-pks (next-method model)
        pks       (if (sequential? pk-or-pks)
                    pk-or-pks
                    [pk-or-pks])]
    (when-not (every? keyword? pks)
      (throw (ex-info (format "Bad %s for model %s: should return keyword or sequence of keywords, got %s"
                              `primary-keys
                              (u/safe-pr-str model)
                              (u/safe-pr-str pk-or-pks))
                      {:model model, :result pk-or-pks})))
    pks))

(m/defmethod primary-keys :default
  [_model]
  [:id])

;;; TODO -- rename to `primary-key-values-map`
(defn primary-key-values
  "Return a map of primary key values for a Toucan 2 `instance`."
  ([instance]
   (primary-key-values (protocols/model instance) instance))
  ([model m]
   (select-keys m (primary-keys model))))

;;; TODO -- consider renaming this to something better. What?
(defn select-pks-fn
  "Return a function to get the value(s) of the primary key(s) from a row, as a single value or vector of values. Used
  by [[toucan2.select/select-pks-reducible]] and thus
  by [[toucan2.select/select-pks-set]], [[toucan2.select/select-pks-vec]], etc.

  The primary keys are determined by [[primary-keys]]. By default this is simply the keyword `:id`."
  [modelable]
  (with-model [model modelable]
    (let [pk-keys (primary-keys model)]
      (if (= (count pk-keys) 1)
        (first pk-keys)
        (apply juxt pk-keys)))))
