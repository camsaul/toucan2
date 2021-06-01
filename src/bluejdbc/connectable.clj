(ns bluejdbc.connectable
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.result-set :as rs]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.transaction :as next.jdbc.transaction]))

;; TODO -- consider whether this should end with a `*` so it's consistent with the other multimethods.
(m/defmulti default-options
  {:arglists '([connectable])}
  u/dispatch-on-first-arg)

(m/defmethod default-options :default
  [connectable]
  {:next.jdbc {:builder-fn (rs/row-builder-fn connectable nil)}})

(m/defmulti default-connectable-for-tableable*
  {:arglists '([tableable options])}
  u/dispatch-on-first-arg)

(m/defmethod default-connectable-for-tableable* :default
  [_ _]
  :bluejdbc/default)

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
  (when (nil? connectable)
    (throw (ex-info "Connectable cannot be nil" {})))
  (when (= connectable :bluejdbc/default)
    (throw (ex-info (format "No default connectable is defined. Define an implementation of connection* for :bluejdbc/default")
                    {})))
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

(defn do-with-connection [connectable tableable options f]
  (let [connectable (or connectable (default-connectable-for-tableable* tableable options))]
    (if (and conn.current/*current-connection*
             (= connectable conn.current/*current-connectable*))
      (f conn.current/*current-connection*)
      (let [options                                        (u/recursive-merge (default-options connectable) options)
            {:keys [^java.sql.Connection connection new?]} (connection connectable options)]
        (binding [conn.current/*current-connectable* connectable
                  conn.current/*current-connection*  connection]
          (if new?
            (with-open [connection connection]
              (f connection))
            (f connection)))))))

;; this can't go in the specs namespace with everything else because it creates a circular reference.
(s/def ::with-connection-arg
  (s/or :connectable (complement vector?)
        :vector      (s/cat :binding     (s/? any?)
                            :connectable (s/? any?)
                            :tableable-options (s/?
                                                (s/alt
                                                 :options (s/cat :options any?)
                                                 :tableable-options (s/cat :tableable any? :options any?))))))

(defn- parse-with-connection-arg [arg]
  (let [parsed (s/conform ::with-connection-arg arg)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret with-connection arg: %s"
                              (s/explain-str ::with-connection-arg arg))
                      {:arg arg})))
    (let [[arg-type arg] parsed
          args (-> (if (= arg-type :connectable)
                     {:connectable arg}
                     arg)
                   (merge (-> arg :tableable-options last))
                   (dissoc :tableable-options))]
      (-> args
          (update :connectable (fn [connectable]
                                 (if (and connectable
                                          (not= connectable '_))
                                   connectable
                                   (if (and (:tableable args)
                                            (= conn.current/*current-connectable* :bluejdbc/default))
                                     (default-connectable-for-tableable* (:tableable args) (:options args))
                                     `conn.current/*current-connectable*))))
          (update :binding #(or % '_))))))

;; TODO -- not sure about this syntax.
(defmacro with-connection
  {:style/indent 1
   :arglists     '([[conn-binding connectable? tableable? options?] & body]
                   [connectable & body])}
  [x & body]
  (let [{:keys [binding connectable tableable options]} (parse-with-connection-arg x)]
    `(do-with-connection
      ~connectable
      ~tableable
      ~options
      (fn [~(vary-meta (or binding '_) assoc :tag 'java.sql.Connection)]
        ~@body))))

(defn parse-connectable-tableable [connectable-tableable]
  (if (sequential? connectable-tableable)
    connectable-tableable
    [conn.current/*current-connectable* connectable-tableable]))

(defn do-with-transaction [connectable options f]
  (with-connection [conn connectable options]
    ;; "ignore" nested transactions -- we'll make it work ourselves.
    (binding [next.jdbc.transaction/*nested-tx* :ignore]
      (next.jdbc/with-transaction [tx-connection conn]
        (let [save-point (.setSavepoint tx-connection)]
          (try
            (binding [conn.current/*current-connection* tx-connection]
              (f conn.current/*current-connection*))
            (catch Throwable e
              (.rollback tx-connection save-point)
              (throw e))))))))

(defmacro with-transaction
  {:style/indent 1
   :arglists     '([[conn-binding connectable options] & body]
                   [connectable & body])}
  [x & body]
  (let [{:keys [binding connectable options]} (parse-with-connection-arg x)]
    `(do-with-transaction
      ~connectable
      ~options
      (fn [~(vary-meta (or binding '_) assoc :tag 'java.sql.Connection)]
        ~@body))))
