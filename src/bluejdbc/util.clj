(ns bluejdbc.util
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [potemkin :as p]
            [pretty.core :refer [PrettyPrintable]]))

(defn keyword-or-class [x]
  (if (keyword? x)
    x
    (class x)))

(defn dispatch-on-first-arg-with [f]
  (fn dispatch-on-first-arg-with*
    ([x]         (f x))
    ([x _]       (f x))
    ([x _ _]     (f x))
    ([x _ _ _]   (f x))
    ([x _ _ _ _] (f x))))

(def ^{:arglists '([a] [a b] [a b c] [a b c d] [a b c d e])} dispatch-on-first-arg
  (dispatch-on-first-arg-with keyword-or-class))

(defn dispatch-on-first-two-args-with [f]
  (fn dispatch-on-first-two-args-with*
    ([x y]       [(f x) (f y)])
    ([x y _]     [(f x) (f y)])
    ([x y _ _]   [(f x) (f y)])
    ([x y _ _ _] [(f x) (f y)])))

(def ^{:arglists '([a b] [a b c] [a b c d] [a b c d e])} dispatch-on-first-two-args
  (dispatch-on-first-two-args-with keyword-or-class))

(defn dispatch-on-first-three-args-with [f]
  (fn dispatch-on-first-three-args-with*
    ([x y z]     [(f x) (f y) (f z)])
    ([x y z _]   [(f x) (f y) (f z)])
    ([x y z _ _] [(f x) (f y) (f z)])))

(def ^{:arglists '([a b] [a b c] [a b c d] [a b c d e])} dispatch-on-first-three-args
  (dispatch-on-first-three-args-with keyword-or-class))

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

(defn pretty-printable-fn [pretty-representation f]
  (reify
    PrettyPrintable
    (pretty [_]
      (pretty-representation))
    clojure.lang.IFn
    (invoke [_]           (f))
    (invoke [_ a]         (f a))
    (invoke [_ a b]       (f a b))
    (invoke [_ a b c]     (f a b c))
    (invoke [_ a b c d]   (f a b c d))
    (invoke [_ a b c d e] (f a b c d e))))
