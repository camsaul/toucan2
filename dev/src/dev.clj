(ns dev
  (:require [bluejdbc.util :as u]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as log.impl]
            [environ.core :as env]))

(def ^:private log-agent
  (agent nil))

(defn- log! [message]
  (send log-agent (fn [_ msg] (println msg)) message))

(defn- logger [namespac]
  (reify log.impl/Logger
    (enabled? [_ level]
      true)
    (write! [_ level e message]
      (let [e (when e
                (u/pprint-to-str (Throwable->map e)))
            s (format "%s [%s] %s%s"
                      (ns-name namespac)
                      (str/upper-case (name level))
                      message
                      (if e
                        (str "\n" e)
                        ""))]
        (log! s)))))

(def ^:private logger-factory
  (reify log.impl/LoggerFactory
    (name [_] "My Logger Factory")
    (get-logger [_ ns]
      (logger ns))))

(alter-var-root #'log/*logger-factory* (constantly logger-factory))

(defn ns-unmap-all
  "Unmap all interned vars in a namespace. Reset the namespace to a blank slate! Perfect for when you rename everything
  and want to make sure you didn't miss a reference or when you redefine a multimethod.

    (ns-unmap-all *ns*)"
  ([]
   (ns-unmap-all *ns*))

  ([a-namespace]
   (doseq [[symb] (ns-interns a-namespace)]
     (ns-unmap a-namespace symb))))

(defn- class->tag
  "Return the `:tag` metadata we should use for a Class. This returns the tag as a vector because array types like
  `String[]` actually need a two-part tag: `'^` and the *string* class name."
  [^Class klass]
  (cond
    ;; String[] -> ['^ "[Ljava.lang.String;"]
    (and (.isArray klass)
         (str/includes? (.getName klass) "."))
    [(symbol "^") (.getName klass)]

    ;; int[] -> ['^ints]
    (.isArray klass)
    [(symbol (str \^ (str/replace (.getCanonicalName klass) #"\[\]$" "s")))]

    ;; java.lang.SomeClass -> ['^SomeClass]
    (re-matches #"^java\.lang\.\w+$" (.getCanonicalName klass))
    [(symbol (str \^ (str/replace (.getCanonicalName klass) #"^java\.lang\.(\w+$)" "$1")))]

    ;; anything.else.SomeClass -> ['^anything.else.SomeClass]
    :else
    [(symbol (str \^ (.getCanonicalName klass)))]))

(defn- type-annotation [x]
  (cond
    (class? x) (class->tag x)
    x          [(symbol x)]))

(defn- generate-proxy-class-method [^java.lang.reflect.Method method proxied-object-symb]
  (let [name         (.getName method)
        param-types  (.getParameterTypes method)
        param-names  (take (count param-types) '[a b c d e f])
        typed-params (mapcat (fn [param-type param-name]
                               (concat (type-annotation param-type) [(symbol param-name)]))
                             param-types param-names)
        args         (vec (cons '_ typed-params))
        return-type  (type-annotation (.getReturnType method))
        form         `(~@return-type ~(symbol name) ~args (~(symbol (str \. name)) ~proxied-object-symb ~@param-names))]
    (println (pr-str form))))

(defn generate-proxy-class-methods
  "Generates methods for an Interface for inclusion in a `deftype`/`defrecord` statement. For interfaces like
  `java.sql.ResultSet` with hundreds of methods this saves a lot of time.

    (generate-proxy-class-methods java.sql.ResultSet 'rs)

    ;; ->
    (^boolean absolute [_ ^int a] (.absolute rs a))
    (^void afterLast [_] (.afterLast rs))
    ...

  Return values and arguments are tagged to avoid ambiguity. With minor tweaks this could be added to a macro!"
  [class-or-classes proxied-object-symb]
  (doseq [^java.lang.reflect.Method method (sort-by #(.getName ^java.lang.reflect.Method %)
                                                    (mapcat #(.getDeclaredMethods ^Class %)
                                                            (if (class? class-or-classes)
                                                              [class-or-classes]
                                                              class-or-classes)))]
    (generate-proxy-class-method method proxied-object-symb)))

(defn set-jdbc-url!
  "Set the JDBC URL used for testing."
  [url]
  (alter-var-root #'env/env assoc :jdbc-url url)
  nil)

;; NOCOMMIT
#_(set-jdbc-url! "jdbc:postgresql://localhost:5432/test-data?user=cam&password=cam")
