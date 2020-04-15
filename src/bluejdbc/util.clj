(ns bluejdbc.util
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [potemkin.collections :as p.collections]
            [potemkin.types :as p.types]))

(defn static-instances
  "Utility function to get the static members of a class. Returns map of `lisp-case` keyword names of members -> value."
  [^Class klass & [{:keys    [target-class
                              name-regex
                              name-transform]
                    namespce :namespace
                    :or      {name-transform identity}}]]
  (into {} (for [^java.lang.reflect.Field f (.getFields klass)
                 :let                       [field-name (.getName f)]
                 :when                      (and (or (nil? target-class)
                                                     (.isAssignableFrom ^Class target-class (.getType f)))
                                                 (or (nil? name-regex)
                                                     (re-find name-regex field-name)))]
             [(keyword (some-> namespce name) (str/lower-case (str/replace (name-transform field-name) #"_" "-")))
              (.get f nil)])))

(defn- enum-value*
  ^Integer [m namespac k not-found]
  (if (integer? k)
    k
    (let [v (get m k ::not-found)]
      (if-not (= v ::not-found)
        v
        (if (and (keyword k)
                 (not (namespace k)))
          (get m (keyword (name namespac) (name k)) not-found)
          not-found)))))

(defn- reverse-lookup* [rev namespac k not-found]
  (cond
    (and (keyword? k) (namespace k))
    k

    (keyword? k)
    (keyword (name namespac) (name k))

    :else
    (get rev k (or not-found k))))

(p.types/defprotocol+ EnumReverseLookup
  (reverse-lookup [m] [m k] [m k not-found]))

(p.types/deftype+ EnumMap [m rev namespac mta]
  EnumReverseLookup
  (reverse-lookup [_]
    rev)

  (reverse-lookup [this k]
    (reverse-lookup this k nil))

  (reverse-lookup [_ k not-found]
    (reverse-lookup* rev namespac k not-found))

  p.collections/AbstractMap
  (get* [_ k default-value]
    (let [result (enum-value* m namespac k default-value)]
      (when (= result default-value nil)
        (throw (ex-info (format "Invalid %s enum %s" (name namespac) (pr-str k)) {})))
      result))

  (assoc* [_ k v]
    (EnumMap. (assoc m k v) (assoc rev v k) namespac mta))

  (dissoc* [_ k]
    (let [v (get m k ::not-found)]
      (EnumMap. (dissoc m k) (if (= v ::not-found) rev (dissoc rev v)) namespac mta)))

  (keys* [_]
    (keys m))

  (meta* [_]
    mta)

  (with-meta* [_ new-meta]
    (EnumMap. m rev namespac new-meta)))

(defn enum-map [m namespac]
  (EnumMap. m
            (zipmap (vals m)
                    (keys m))
            namespac
            (meta m)))

(defmacro define-enums [symb klass & [re & {:as options}]]
  `(def ~(with-meta symb {:arglists ''(^Integer [k] ^Integer [k not-found])})
     ~(format "Map of %s enum values, namespaced keyword -> int." symb)
     (let [options# ~(merge {:namespace      (keyword symb)
                             :name-regex     re
                             :name-transform (if re
                                               `(fn [~'s]
                                                  (str/replace ~'s ~re ""))
                                               identity)}
                            options)
           m#       (static-instances ~klass options#)]
       (enum-map m# (:namespace options#)))))

#_(defmacro with-class-when-available [[class-binding class-name] & body]
  `(when-let [class# (try
                       (Class/forName "org.postgresql.jdbc.PgResultSet")
                       (catch Throwable ~'_))]
     (let [~(vary-meta class-binding assoc :tag 'java.lang.Class) class#]
       ~@body)))

(defn parse-currency
  "Parse a currency String to a BigDecimal. Handles a variety of different formats, such as:

    $1,000.00
    -£127.54
    -127,54 €
    kr-127,54
    € 127,54-
    ¥200"
  ^java.math.BigDecimal [^String s]
  (when-not (str/blank? s)
    (bigdec
     (reduce
      (partial apply str/replace)
      s
      [
       ;; strip out any current symbols
       [#"[^\d,.-]+"          ""]
       ;; now strip out any thousands separators
       [#"(?<=\d)[,.](\d{3})" "$1"]
       ;; now replace a comma decimal seperator with a period
       [#","                  "."]
       ;; move minus sign at end to front
       [#"(^[^-]+)-$"         "-$1"]]))))

(defn pprint-to-str [x]
  (with-open [w (java.io.StringWriter.)]
    (binding [pprint/*print-right-margin* 120]
      (pprint/pprint x w))
    (str w)))
