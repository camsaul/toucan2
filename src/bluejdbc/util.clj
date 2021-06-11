(ns bluejdbc.util
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [potemkin :as p]
            [pretty.core :as pretty]))

(defn qualify-symbol-for-*ns* [symb]
  (let [qualified (pretty/qualify-symbol-for-*ns* symb)]
    (if (not= qualified symb)
      qualified
      (let [aliases             (ns-aliases *ns*)
            ns-symb->alias      (zipmap (map ns-name (vals aliases))
                                        (keys aliases))
            bluejdbc-core-alias (get ns-symb->alias 'bluejdbc.core)]
        (symbol (if bluejdbc-core-alias
                  (name bluejdbc-core-alias)
                  "bluejdbc.core")
                (name symb))))))

(defmethod m.combo.threaded/threading-invoker :second
  [_]
  (fn
    ([a b]            [b (fn [method b*] (method a b*))])
    ([a b c]          [b (fn [method b*] (method a b* c))])
    ([a b c d]        [b (fn [method b*] (method a b* c d))])
    ([a b c d & more] [b (fn [method b*] (apply method a b* c d more))])))

(defmethod m.combo.threaded/threading-invoker :third
  [_]
  (fn
    ([a b c]          [c (fn [method c*] (method a b c*))])
    ([a b c d]        [c (fn [method c*] (method a b c* d))])
    ([a b c d & more] [c (fn [method c*] (apply method a b c* d more))])))

(defmethod m.combo.threaded/threading-invoker :fourth
  [_]
  (fn
    ([a b c d]        [d (fn [method d*] (method a b c d*))])
    ([a b c d & more] [d (fn [method d*] (apply method a b c d* more))])))

(p/defprotocol+ DispatchValue
  :extend-via-metadata true
  (dispatch-value [this]))

(extend-protocol DispatchValue
  clojure.lang.Keyword
  (dispatch-value [k]
    k)

  Object
  (dispatch-value [this]
    (type this))

  nil
  (dispatch-value [_]
    nil))

(p/defrecord+ DispatchOn [x dv]
  DispatchValue
  (dispatch-value [_] dv)

  pretty/PrettyPrintable
  (pretty [_]
    (list (qualify-symbol-for-*ns* `dispatch-on) x dv)))

(defn dispatch-on [x dispatch-value]
  (->DispatchOn x dispatch-value))

(defn dispatch-on? [x]
  (instance? DispatchOn x))

(defn unwrap-dispatch-on [x]
  (if (dispatch-on? x)
    (:x x)
    x))

(defn dispatch-on-first-arg-with [f]
  (fn dispatch-on-first-arg-with*
    ([x]           (f x))
    ([x _]         (f x))
    ([x _ _]       (f x))
    ([x _ _ _]     (f x))
    ([x _ _ _ _]   (f x))
    ([x _ _ _ _ _] (f x))))

(def ^{:arglists '([a] [a b] [a b c] [a b c d] [a b c d e] [a b c d e f])} dispatch-on-first-arg
  (dispatch-on-first-arg-with dispatch-value))

(defn dispatch-on-first-two-args-with [f]
  (fn dispatch-on-first-two-args-with*
    ([x y]         [(f x) (f y)])
    ([x y _]       [(f x) (f y)])
    ([x y _ _]     [(f x) (f y)])
    ([x y _ _ _]   [(f x) (f y)])
    ([x y _ _ _ _] [(f x) (f y)])))

(def ^{:arglists '([a b] [a b c] [a b c d] [a b c d e] [a b c d e f])} dispatch-on-first-two-args
  (dispatch-on-first-two-args-with dispatch-value))

(defn dispatch-on-first-three-args-with [f]
  (fn dispatch-on-first-three-args-with*
    ([x y z]       [(f x) (f y) (f z)])
    ([x y z _]     [(f x) (f y) (f z)])
    ([x y z _ _]   [(f x) (f y) (f z)])
    ([x y z _ _ _] [(f x) (f y) (f z)])))

(def ^{:arglists '([a b c] [a b c d] [a b c d e] [a b c d e f])} dispatch-on-first-three-args
  (dispatch-on-first-three-args-with dispatch-value))

(defn dispatch-on-first-four-args-with [f]
  (fn dispatch-on-first-four-args-with*
    ([a b c d]     [(f a) (f b) (f c) (f d)])
    ([a b c d _]   [(f a) (f b) (f c) (f d)])
    ([a b c d _ _] [(f a) (f b) (f c) (f d)])))

(def ^{:arglists '([a b c d] [a b c d e] [a b c d e f])} dispatch-on-first-four-args
  (dispatch-on-first-four-args-with dispatch-value))

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

;; TODO -- should this be part of the pretty lib?
(defn pretty-printable-fn [pretty-representation-fn f]
  (reify
    pretty/PrettyPrintable
    (pretty [_]
      (pretty-representation-fn))
    clojure.lang.IFn
    (invoke [_]           (f))
    (invoke [_ a]         (f a))
    (invoke [_ a b]       (f a b))
    (invoke [_ a b c]     (f a b c))
    (invoke [_ a b c d]   (f a b c d))
    (invoke [_ a b c d e] (f a b c d e))))

(defn recursive-merge
  ([m]
   m)

  ([m1 m2]
   (merge-with (fn [x y]
                 (if (map? x)
                   (recursive-merge x y)
                   y))
               m1 m2))

  ([m1 m2 & more]
   (apply recursive-merge (recursive-merge m1 m2) more)))

(defn quit-early-exception [x]
  (throw (ex-info "Quit early" {::quit-early x})))

(defn do-returning-quit-early [thunk]
  (try
    (thunk)
    (catch Throwable e
      (or (loop [e e]
            (if-let [x (get (ex-data e) ::quit-early)]
              x
              (when-let [cause (ex-cause e)]
                (recur cause))))
          (throw e)))))

(defn implementable-dispatch-values
  "Return a sequence of dispatch values to suggest someone implement when there's no matching method."
  ([x]
   (let [x (some-> x dispatch-value)]
     (for [x (if x [x :default] [:default])]
       [x])))

  ([x & more]
   (for [x-dv    (implementable-dispatch-values x)
         more-dv (apply implementable-dispatch-values more)]
     (vec (concat x-dv more-dv)))))

(defn suggest-dispatch-values
  "Generate a string suggesting dispatch values to implement methods for. For throwing exceptions in `:default`
  implementations when there's no matching method."
  [& dispatch-values]
  (str "Add an impl for " (str/join " or " (apply implementable-dispatch-values dispatch-values))))

(defn maybe-derive
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))
