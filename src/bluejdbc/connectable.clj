(ns bluejdbc.connectable
  (:require [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.result-set :as jdbc.rs]))

(m/defmulti connection*
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg)

(m/defmethod connection* :around :default
  [connectable {:keys [include-connection-info-in-exceptions?]
                :or   {include-connection-info-in-exceptions? false}
                :as   options}]
  (try
    (let [m (next-method connectable options)]
      (when-not (and (map? m)
                     (contains? m :connection)
                     (contains? m :new?)
                     (let [{:keys [connection]} m]
                       (or (not connection)
                           (instance? java.sql.Connection connection))))
        (throw (ex-info "connection* should return a map with `:connection` and `:new?`."
                        {:returned m})))
      m)
    (catch Throwable e
      (throw (ex-info (ex-message e)
                      (if include-connection-info-in-exceptions?
                        {:connectable connectable
                         :options     options}
                        {})
                      e)))))

(m/defmethod connection* :default
  [connectable options]
  (when (keyword? connectable)
    (throw (ex-info (format "Unknown connectable %s. Did you define a connection* method for it?" connectable)
                    {:k connectable})))
  {:connection  (next.jdbc/get-connection connectable)
   :new?        true
   :options     options})

(m/defmethod connection* java.sql.Connection
  [conn options]
  {:connection  conn
   :new?        true
   :options     options})

(defn connection
  ([k]
   (connection* k nil))

  ([k options]
   (connection* k options)))

(def ^:dynamic *connectable*
  :default-connectable)

(def ^:dynamic ^java.sql.Connection *connection*
  nil)

(def ^:dynamic *options*
  nil)

;; TODO -- maybe this belongs somewhere more visible e.g. util
(def default-options
  {:execute {:builder-fn jdbc.rs/as-unqualified-maps}
   :rf      u/default-rf
   :init    []})

(defn do-with-connection [connectable options f]
  (if (= connectable *connectable*)
    (let [options (u/recursive-merge default-options *options* options)]
      (binding [*options* options]
        (f *connection* options)))
    (let [options                                                (u/recursive-merge default-options options)
          {:keys [^java.sql.Connection connection new? options]} (connection connectable options)]
      (binding [*connectable* connectable
                *connection*  connection
                *options*     options]
        (if new?
          (with-open [connection connection]
            (f connection options))
          (f connection options))))))

;; TODO -- not sure about this syntax.
(defmacro with-connection
  {:arglists '([[[conn-binding options-binding] connectable options] & body]
               [[_ connectable options] & body]
               [_ & body])}
  [x & body]
  (let [[conn-options-bindings connectable options] (if (sequential? x)
                                                      x
                                                      [nil x nil])]
    `(do-with-connection
      ~connectable ~options
      ~(cond
         (sequential? conn-options-bindings)
         (let [[conn-binding options-binding] conn-options-bindings]
           `(fn [~(vary-meta (or conn-binding '_) assoc :tag 'java.sql.Connection)
                 ~(or options-binding '_)]
              ~@body))

         (and conn-options-bindings
              (not= conn-options-bindings '_))
         `(fn [conn# options#]
            (let [~conn-options-bindings [conn# options#]]
              ~@body))

         :else
         `(fn [~'_ ~'_]
            ~@body)))))
