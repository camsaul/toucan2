(ns toucan2.model
  (:require
   [methodical.core :as m]
   [toucan2.current :as current]
   [toucan2.model :as model]
   [toucan2.util :as u]
   [toucan2.connection :as conn]
   [pretty.core :as pretty]))

;; TODO -- this should probably also support.
(m/defmulti do-with-model
  {:arglists '([modelable f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-model :default
  [model f]
  (f model))

(defmacro with-model [[model-binding modelable] & body]
  `(do-with-model ~modelable (^:once fn* [model#]
                              (binding [current/*model* model#]
                                (let [~model-binding model#]
                                  ~@body)))))

(m/defmulti default-connectable
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod default-connectable :default
  [_model]
  :toucan/default)

(defn- current-connectable [model]
  (u/with-debug-result (format "Realizing deferred current connectable for model %s. %s is bound to %s"
                               (pr-str model)
                               `current/*connection*
                               (pr-str current/*connection*))
    (if (= current/*connection* :toucan/default)
      (u/with-debug-result (format "Current connection is default; using %s for model" `default-connectable)
        (default-connectable model))
      (u/with-debug-result "Current connection is *NOT* default. Using that"
        current/*connection*))))

(defrecord DeferredCurrentConnectable [model]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `deferred-current-connectable model)))

(m/defmethod conn/do-with-connection DeferredCurrentConnectable
  [{:keys [model]} f]
  (conn/do-with-connection (current-connectable model) f))

(defn deferred-current-connectable [model]
  (->DeferredCurrentConnectable model))

(m/defmulti table-name
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod table-name :default
  [model]
  (name model))

(m/defmulti primary-keys
  {:arglists '([model])}
  u/dispatch-on-first-arg)

;; if the PK comes back unwrapped, wrap it.
(m/defmethod primary-keys :around :default
  [model]
  (let [pk (next-method model)]
    (if (sequential? pk)
      pk
      [pk])))

(m/defmethod primary-keys :default
  [_model]
  [:id])
