(ns bluejdbc.connectable
  (:require [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.result-set :as jdbc.rs]))

;; TODO -- consider whether this should end with a `*` so it's consistent with the other multimethods.
(m/defmulti default-options
  {:arglists '([connectable])}
  u/dispatch-on-first-arg)

(m/defmethod default-options :default
  [_]
  {:execute {:builder-fn jdbc.rs/as-unqualified-maps}
   :rf      u/default-rf
   :init    []})

(m/defmulti connection*
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg)

;; TODO -- not sure if `include-connection-info-in-exceptions?` should be something in the options map or be its own
;; dynamic variable for debug purposes.

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

(defn do-with-connection [connectable options f]
  (if (= connectable *connectable*)
    (f *connection*)
    (let [options                                                (u/recursive-merge (default-options connectable) options)
          {:keys [^java.sql.Connection connection new? options]} (connection connectable options)]
      (binding [*connectable* connectable
                *connection*  connection]
        (if new?
          (with-open [connection connection]
            (f connection))
          (f connection))))))

;; TODO -- not sure about this syntax.
(defmacro with-connection
  {:arglists '([[conn-binding connectable options] & body]
               [connectable & body])}
  [x & body]
  (let [[conn-binding connectable options] (if (sequential? x)
                                             x
                                             [nil x nil])]
    `(do-with-connection
      ~connectable ~options
      (fn [~(vary-meta (or conn-binding '_) assoc :tag 'java.sql.Connection)]
        ~@body))))
