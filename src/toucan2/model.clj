(ns toucan2.model
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
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
                (f model))))

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
  :default)

(defn- current-connectable [model]
  (u/with-debug-result ["Realizing deferred current connectable for model %s." model]
    (u/println-debug ["%s is %s" `conn/*current-connectable* conn/*current-connectable*])
    (or conn/*current-connectable*
        (do
          (u/println-debug ["Using %s for model %s" `default-connectable model])
          (default-connectable model)))))

(defrecord DeferredCurrentConnectable [model]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `deferred-current-connectable model)))

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
    (throw (ex-info (format "Invalid model %s: don't know how to get its table name." (pr-str model))
                    {:model model}))))

(m/defmethod table-name clojure.lang.Named
  [model]
  (name model))

(m/defmethod table-name String
  [table]
  table)

(m/defmulti primary-keys
  "Return a sequence of the primary key columns names, as keywords, for a model."
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
                              (pr-str model)
                              (pr-str pk-or-pks))
                      {:model model, :result pk-or-pks})))
    pks))

(m/defmethod primary-keys :default
  [_model]
  [:id])

(defn primary-keys-vec
  "Get the primary keys for a `model` as a vector."
  [model]
  (let [pk-keys (primary-keys model)]
    (if (sequential? pk-keys)
      pk-keys
      [pk-keys])))

;;; TODO -- rename to `primary-key-values-map`
(defn primary-key-values
  "Return a map of primary key values for a Toucan 2 `instance`."
  ([instance]
   (primary-key-values (instance/model instance) instance))
  ([model m]
   (select-keys m (primary-keys-vec model))))

(defn select-pks-fn
  "Return a function to get the value(s) of the primary key(s) from a row. Used by [[toucan2.select/select-pks-reducible]]
  and thus by [[toucan2.select/select-pks-set]], [[toucan2.select/select-pks-vec]], etc.

  The primary keys are determined by [[toucan2.model/primary-keys]]. By default this is simply the keyword `:id`."
  [modelable]
  (with-model [model modelable]
    (let [pk-keys (primary-keys model)]
      (if (= (count pk-keys) 1)
        (first pk-keys)
        (apply juxt pk-keys)))))
