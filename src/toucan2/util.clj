(ns toucan2.util
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [pretty.core :as pretty]
   [puget.printer :as puget]
   [toucan2.protocols :as protocols]))

(set! *warn-on-reflection* true)

;;; TODO -- maybe we should use an ordered map here so keys will be kept in the order they were added.
(def ^:dynamic *error-context* {})

(def ^:dynamic *debug* false)

(def ^:private ^:dynamic *debug-indent-level* 0)

(def ^:private ^:dynamic *last-indentation-separator* "| ")

(defn- indentation []
  (str/join (concat (repeat (dec *debug-indent-level*) "|  ")
                    (when (pos? *debug-indent-level*)
                      (str *last-indentation-separator* \space)))))

(defn ^:no-doc println-debug-lines [s]
  (let [[first-line & more] (str/split-lines (str/trim s))]
    (when first-line
      (binding [*last-indentation-separator* "+-"]
        (print (indentation)))
      (println first-line))
    (doseq [line more]
      (print (indentation))
      (println line))))

(defn ^:no-doc println-debug* [& args]
  (println-debug-lines (with-out-str (apply println args))))

(def ^:private ^:dynamic *color*
  "Whether or not to print the trace in color. True by default, unless the env var `NO_COLOR` is true."
  (if-let [env-var-value (System/getenv "NO_COLOR")]
    (Boolean/parseBoolean env-var-value)
    true))

(defmulti ^:private print-handler
  {:arglists '([klass])}
  (fn [klass]
    klass))

(defmethod print-handler :default
  [_klass]
  nil)

(defmethod print-handler pretty.core.PrettyPrintable
  [_klass]
  (fn [printer x]
    (puget/format-doc printer (pretty/pretty x))))

(defmethod print-handler clojure.core.Eduction
  [_klass]
  (fn [printer ^clojure.core.Eduction ed]
    (puget/format-doc printer (list 'eduction (.xform ed) (.coll ed)))))

(defmethod print-handler clojure.lang.IRecord
  [klass]
  (when (isa? klass clojure.lang.IReduceInit)
    (fn [_printer x]
      [:text (str x)])))

(defrecord ^:no-doc Doc [forms])

(defmethod print-handler Doc
  [_klass]
  (fn [printer {:keys [forms]}]
    (into [:group] (for [form forms]
                     (puget/format-doc printer form)))))

(defrecord ^:no-doc Text [s])

(defmethod print-handler Text
  [_klass]
  (fn [_printer {:keys [s]}]
    [:span
     [:text s]
     [:line]]))

(prefer-method print-handler pretty.core.PrettyPrintable clojure.lang.IRecord)

(defn- default-color-printer [x]
  ;; don't print in black. I can't see it
  (puget/cprint x {:color-scheme   {:nil nil}
                   :print-handlers print-handler}))

(defn- default-boring-printer [x]
  (puget/pprint x {:print-handlers print-handler}))

(defn- pretty-printer []
  (if *color*
    default-color-printer
    default-boring-printer))

(defn- pprint
  "Pretty print a form `x`."
  [x]
  (try
    ((pretty-printer) x)
    (catch Throwable e
      (throw (ex-info (format "Error pretty printing %s: %s" (some-> x class .getCanonicalName) (ex-message e))
                      {:context *error-context*, :object x}
                      e)))))

(defn ^:no-doc pprint-to-str [x]
  (str/trim (with-out-str (pprint x))))

(defn- interleave-all
  "Exactly like [[interleave]] but includes the entirety of both collections even if the other collection is shorter. If
  one collection 'runs out', the remaining elements of the other collection are appended directly to the end of the
  resulting collection."
  [x y]
  (loop [acc [], x x, y y]
    (let [acc (cond-> acc
                (seq x) (conj (first x))
                (seq y) (conj (first y)))
          x   (next x)
          y   (next y)]
      (if (and (empty? x) (empty? y))
        acc
        (recur acc x y)))))

(defmacro ^:no-doc format-doc
  "Convert `format-string` and `args` into something that can be pretty-printed by puget."
  [format-string & args]
  (let [->Text (fn [s]
                 (list `->Text (str/trimr s)))
        texts  (map ->Text (str/split format-string #"%s"))]
    `(pprint-to-str (->Doc ~(vec (interleave-all texts args))))))

(defmacro ^:no-doc println-debug
  [arg & more]
  `(when *debug*
     ~(if (vector? arg)
        `(println-debug-lines ~(if (= (count arg) 1)
                                 `(pprint-to-str ~(first arg))
                                 `(format-doc ~(first arg) ~@(rest arg)))
                              ~@more)
        `(println-debug* ~arg ~@more))))

(defn- print-debug-result [result]
  (print (indentation))
  (print "↳ ")

  (let [s     (pprint-to-str result)
        lines (str/split-lines (str/trim s))]
    (print (first lines))
    (doseq [line (rest lines)]
      (print \newline)
      (print (indentation))
      (print "  ")
      (print line))
    (println)))

(defn ^:no-doc do-with-debug-result [message thunk]
  (println-debug message)
  (let [result (binding [*debug-indent-level* (inc *debug-indent-level*)]
                 (thunk))]
    (print-debug-result result)
    result))

(defmacro ^:no-doc with-debug-result [message & body]
  (if (vector? message)
    `(with-debug-result ~(if (> (count message) 1)
                           `(format-doc ~(first message) ~@(rest message))
                           `(pprint-to-str ~(first message)))
       ~@body)
    `(let [thunk# (^:once fn* [] ~@body)]
       (if-not *debug*
         (thunk#)
         (do-with-debug-result ~message thunk#)))))

(defn dispatch-on-first-arg
  "Dispatch on the first argument using [[dispatch-value]], and ignore all other args."
  [x & _]
  (protocols/dispatch-value x))

(defn dispatch-on-first-two-args
  "Dispatch on the two arguments using [[protocols/dispatch-value]], and ignore all other args."
  [x y & _]
  [(protocols/dispatch-value x) (protocols/dispatch-value y)])

(defn dispatch-on-first-three-args
  "Dispatch on the three arguments using [[protocols/dispatch-value]], and ignore all other args."
  [x y z & _]
  [(protocols/dispatch-value x) (protocols/dispatch-value y) (protocols/dispatch-value z)])

(defn lower-case-en
  "Locale-agnostic version of [[clojure.string/lower-case]]. `clojure.string/lower-case` uses the default locale in
  conversions, turning `ID` into `ıd`, in the Turkish locale. This function always uses the `Locale/US` locale."
  [^CharSequence s]
  (.. s toString (toLowerCase (java.util.Locale/US))))

(defn maybe-derive
  "Derive `child` from `parent` only if `child` is not already a descendant of `parent`."
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))

(defprotocol ^:no-doc SafePRStr
  (^:private safe-printable [this]
    "Convert `this` to a safe representation for printing by [[safe-pr-str]]."))

(defn- walk-safe-printable [x]
  (walk/postwalk safe-printable x))

(defn ^:no-doc safe-pr-str
  "Like [[clojure.core/pr-str]], but does not evaluate reducibles or eductions, and
  handles [[pretty.core/PrettyPrintable]] things recursively."
  [x]
  (pr-str (walk-safe-printable x)))

(extend-protocol SafePRStr
  nil
  (safe-printable [this]
    this)

  Object
  (safe-printable [this]
    this)

  pretty.core.PrettyPrintable
  (safe-printable [this]
    (walk-safe-printable (pretty/pretty this)))

  clojure.core.Eduction
  (safe-printable [^clojure.core.Eduction ed]
    (walk-safe-printable (list 'eduction (.xform ed) (.coll ed)))))

(defprotocol ^:private AddContext
  (^:no-doc add-context ^Throwable [^Throwable e additional-context]))

(defn- add-context-to-ex-data [ex-data-m additional-context]
  (-> ex-data-m
      #_(update :toucan2/old-context merge *error-context*)
      (update :toucan2/context-trace #(conj (vec %) (walk/prewalk
                                                     (fn [form]
                                                       (cond
                                                         (instance? pretty.core.PrettyPrintable form)
                                                         (pretty/pretty form)

                                                         (instance? clojure.core.Eduction form)
                                                         (list 'eduction
                                                               (.xform ^clojure.core.Eduction form)
                                                               (.coll ^clojure.core.Eduction form))

                                                         (and (instance? clojure.lang.IReduceInit form)
                                                              (not (coll? form)))
                                                         (class form)

                                                         :else form))
                                                     additional-context)))))

(extend-protocol AddContext
  clojure.lang.ExceptionInfo
  (add-context [^Throwable e additional-context]
    (doto ^Throwable (ex-info (ex-message e)
                              (add-context-to-ex-data (ex-data e) additional-context)
                              (ex-cause e))
      (.setStackTrace (.getStackTrace e))))

  Throwable
  (add-context [^Throwable e additional-context]
    (doto ^Throwable (ex-info (ex-message e)
                              (add-context-to-ex-data {} additional-context)
                              e)
      (.setStackTrace (.getStackTrace e)))))

(defmacro try-with-error-context
  {:style/indent :defn}
  [additional-context & body]
  `(try
     ~@body
     (catch Throwable e#
       (throw (add-context e# ~additional-context)))))
