(ns bluejdbc.util
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [potemkin :as p]))

(defn keyword-or-class [x]
  (if (keyword? x)
    x
    (class x)))

(defn dispatch-on-first-arg
  ([x]         (keyword-or-class x))
  ([x _]       (keyword-or-class x))
  ([x _ _]     (keyword-or-class x))
  ([x _ _ _]   (keyword-or-class x))
  ([x _ _ _ _] (keyword-or-class x)))

(defn dispatch-on-first-two-args
  ([x y]       [(keyword-or-class x) (keyword-or-class y)])
  ([x y _]     [(keyword-or-class x) (keyword-or-class y)])
  ([x y _ _]   [(keyword-or-class x) (keyword-or-class y)])
  ([x y _ _ _] [(keyword-or-class x) (keyword-or-class y)]))

(defn dispatch-on-first-three-args
  ([x y z]     [(keyword-or-class x) (keyword-or-class y) (keyword-or-class z)])
  ([x y z _]   [(keyword-or-class x) (keyword-or-class y) (keyword-or-class z)])
  ([x y z _ _] [(keyword-or-class x) (keyword-or-class y) (keyword-or-class z)]))

(p/defprotocol+ CoerceToProperties
  "Protocol for anything that can be coerced to an instance of `java.util.Properties`."
  (->Properties ^java.util.Properties [this]
    "Coerce `this` to a `java.util.Properties`."))

(extend-protocol CoerceToProperties
  nil
  (->Properties [_]
    nil)

  java.util.Properties
  (->Properties [this]
    this)

  clojure.lang.IPersistentMap
  (->Properties [m]
    (let [properties (java.util.Properties.)]
      (doseq [[k v] m]
        (.setProperty properties (name k) (if (keyword? v)
                                            (name v)
                                            (str v))))
      properties)))

(defn assert-no-recurs
  "Throw an Exception if there are any `recur` forms in `form`."
  [message form]
  (walk/postwalk
   (fn [form]
     (when (and (seqable? form)
                (symbol? (first form))
                (= (first form) 'recur))
       (throw (ex-info (str "recur is not allowed inside " message) {:form form})))
     form)
   form))

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

(defn pprint-to-str
  "Pretty-print `x` to a string. Mostly used for log message purposes."
  ^String [x]
  (with-open [w (java.io.StringWriter.)]
    (binding [pprint/*print-right-margin* 120]
      (pprint/pprint x w))
    (str w)))

(defn qualified-name
  "Return `k` as a string, qualified by its namespace, if any (unlike `name`). Handles `nil` values gracefully as well
  (also unlike `name`).

     (u/qualified-name :type/FK) -> \"type/FK\""
  [k]
  (when (some? k)
    (if-let [namespac (when (instance? clojure.lang.Named k)
                        (namespace k))]
      (str namespac "/" (name k))
      (name k))))
