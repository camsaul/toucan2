(ns bluejdbc.util.macros.proxy-class
  (:require [clojure.string :as str]
            [potemkin.types :as p.types])
  (:import java.lang.reflect.Method))

(defn- class->tag
  "Return the `:tag` metadata we should use for a Class."
  [^Class klass]
  (cond
    ;; String[] -> "[Ljava.lang.String;"
    (and (.isArray klass)
         (str/includes? (.getName klass) "."))
    (.getName klass)

    ;; int[] -> 'ints
    (.isArray klass)
    (symbol (str/replace (.getCanonicalName klass) #"\[\]$" "s"))

    ;; anything.else.SomeClass -> 'anything.else.SomeClass
    :else
    (symbol (.getCanonicalName klass))))

(defn- type-annotation [x]
  (cond
    (class? x) (class->tag x)
    x          (symbol x)))

(defn- proxy-class-method [^Method method proxied-object-symb]
  (let [name             (.getName method)
        return-type      (type-annotation (.getReturnType method))
        param-types      (.getParameterTypes method)
        param-names      (take (count param-types) '[a b c d e f g h])
        typed-params     (map (fn [param-type param-name]
                                (with-meta (symbol param-name) {:tag (type-annotation param-type)}))
                              param-types param-names)
        args             (vec (cons '_ typed-params))
        method-body-form `(~(symbol (str \. name)) ~proxied-object-symb ~@param-names)]
    `(~(with-meta (symbol name) {:tag return-type})
      ~args
      ~method-body-form)))

(defn- all-interfaces [^Class interface]
  {:pre [(class? interface)]}
  (filter distinct? (cons interface (mapcat all-interfaces (.getInterfaces interface)))))

(defn- proxy-class-methods
  "Generates methods for an Interface for inclusion in a `deftype`/`defrecord` statement. For interfaces like
  `java.sql.ResultSet` with hundreds of methods this saves a lot of time.

    (generate-proxy-class-methods java.sql.ResultSet 'rs)

    ;; ->
    ((^boolean absolute [_ ^int a] (.absolute rs a))
     (^void afterLast [_] (.afterLast rs))
     ...)

  Return values and arguments are tagged to avoid ambiguity."
  [interface proxied-object-symb]
  {:pre [(class? interface)]}
  (for [^Method method (sort-by #(.getName ^Method %)
                                (mapcat #(.getDeclaredMethods ^Class %)
                                        (all-interfaces interface)))]
    (proxy-class-method method proxied-object-symb)))

(defn- parse-body
  "Parse the body of a `deftype`/`defprotocol` form into a map of interface/protocol -> method forms."
  [interfaces-and-methods]
  (transduce
   identity
   (fn
     ([]
      [{} nil])

     ([[m]]
      m)

     ([[m current-interface] form]
      (if (symbol? form)
        [m form]
        [(update m current-interface concat [form]) current-interface])))
   interfaces-and-methods))

(defn- arglist-meta-tags
  "Sequence of tags applied to `arglist` for the purposes of comparing two method bodies."
  [arglist]
  (mapv (fn [symb]
          (let [{:keys [tag]} (meta symb)]
            (str (or (try
                       (when-let [resolved (resolve tag)]
                         (when (class? resolved)
                           (.getCanonicalName ^Class resolved)))
                       (catch Throwable _))
                     tag
                     "java.lang.Object"))))
        arglist))

(defn- splice-method-lists
  "Splice two sequcences of methods forms together."
  [& method-lists]
  (vals (sort-by key (into {} (for [list                           method-lists
                                    [method-name arglist :as form] list
                                    :let                           [signature [method-name (arglist-meta-tags arglist)]]]
                                [signature form])))))

(defn- splice-methods-into-body [body interface method-forms]
  (let [protocol->methods (update (parse-body body) interface (fn [overrides] (splice-method-lists method-forms overrides)))]
    (mapcat (fn [[protocol method-forms]]
              (cons protocol method-forms))
            protocol->methods)))

(defmacro define-proxy-class
  "Define a proxy type that implements `interface`, where `proxied-obj-field` is the proxied object.

    (define-proxy-class ProxyConnection java.sql.Connection [conn])
    ;; ->
    (deftype ProxyConnection [^java.sql.Connection conn]
      java.sql.Connection
      (prepareStatement [_] (.prepareStatement conn))
      ...)"
  ;; TODO -- not sure if right
  {:style/indent '(3 nil nil (:defn))}
  [class-name interface [proxied-obj-field & other-fields] & body]
  {:pre [(symbol? class-name) (symbol? interface) (symbol? proxied-obj-field) (every? symbol? other-fields)]}
  (let [resolved-interface     (resolve interface)
        ^Class interface-class (cond
                                 (class? resolved-interface) resolved-interface
                                 (var? resolved-interface)   (:on-interface (var-get resolved-interface)))
        _                      (when-not (instance? Class interface-class)
                                 (throw (ex-info (format "Invalid interface: cannot resolve class %s"
                                                         (pr-str interface))
                                                 {:interface interface})))
        proxy-methods          (proxy-class-methods interface-class proxied-obj-field)]
    `(p.types/deftype+ ~class-name [~(vary-meta proxied-obj-field assoc :tag (symbol (.getCanonicalName interface-class)))
                                    ~@other-fields]
       ~@(splice-methods-into-body body interface proxy-methods))))
