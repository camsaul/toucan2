(ns toucan2.util
  (:require
   [clojure.string :as str]
   [potemkin :as p]
   [pretty.core :as pretty]
   [puget.printer :as puget]))

(set! *warn-on-reflection* true)

(def ^:dynamic *debug* false)

(def ^:dynamic *debug-indent-level* 0)

(defn indentation []
  (str/join (repeat *debug-indent-level* "|  ")))

(defn println-debug-lines [s]
  (let [lines       (str/split-lines (str/trim s))]
    (doseq [line lines]
      (print (indentation))
      (println line))))

(defn println-debug* [& args]
  (println-debug-lines (with-out-str (apply println args))))

(def ^:dynamic *color*
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

(prefer-method print-handler pretty.core.PrettyPrintable clojure.lang.IRecord)

(defn- default-color-printer [x]
  ;; don't print in black. I can't see it
  (puget/cprint x {:color-scheme   {:nil nil}
                   :print-handlers print-handler}))

(defn- default-boring-printer [x]
  (puget/pprint x {:print-handlers print-handler}))

(defn pretty-printer []
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
                      {:object x}
                      e)))))

(defn pprint-to-str [x]
  (str/trim (with-out-str (pprint x))))

(defmacro println-debug
  [arg & more]
  `(when *debug*
     ~(if (vector? arg)
        `(println-debug-lines ~(if (= (count arg) 1)
                                 `(pprint-to-str ~(first arg))
                                 `(format ~(first arg) ~@(for [arg (rest arg)]
                                                           `(pprint-to-str ~arg))))
                              ~@more)
        `(println-debug* ~arg ~@more))))

(defn print-debug-result [result]
  (print (indentation))
  (print "↳ ")

  (let [s     (pprint-to-str result)
        lines (str/split-lines (str/trim s))]
    (print (first lines))
    (doseq [line (rest lines)]
      (print \newline)
      (print (indentation))
      (print "   ")
      (print line))
    (println)))

(defn do-with-debug-result [message thunk]
  (println-debug message)
  (let [result (binding [*debug-indent-level* (inc *debug-indent-level*)]
                 (thunk))]
    (print-debug-result result)
    result))

(defmacro with-debug-result [message & body]
  (if (vector? message)
    `(with-debug-result ~(if (> (count message) 1)
                           `(format ~(first message)
                                   ~@(for [arg (rest message)]
                                       `(pprint-to-str ~arg)))
                           `(pprint-to-str ~(first message)))
       ~@body)
    `(let [thunk# (^:once fn* [] ~@body)]
       (if-not *debug*
         (thunk#)
         (do-with-debug-result ~message thunk#)))))

;; TODO -- consider renaming to dispatch-value
(defn keyword-or-type [x]
  (if (keyword? x)
    x
    (type x)))

(p/defprotocol+ DispatchValue
  (dispatch-value [x]))

(extend-protocol DispatchValue
  Object
  (dispatch-value [x]
    (type x))

  nil
  (dispatch-value [_nil]
    nil)

  clojure.lang.Keyword
  (dispatch-value [k]
    k))

(defn dispatch-on-first-arg
  [x & _]
  (dispatch-value x))

(defn dispatch-on-second-arg
  [_ y & _]
  (dispatch-value y))

(defn dispatch-on-first-two-args
  [x y & _]
  [(dispatch-value x) (dispatch-value y)])

(defn dispatch-on-first-three-args
  [x y z & _]
  [(dispatch-value x) (dispatch-value y) (dispatch-value z)])

(defn lower-case-en
  "Locale-agnostic version of [[clojure.string/lower-case]]. `clojure.string/lower-case` uses the default locale in conversions, turning `ID` into `ıd`, in the Turkish locale.
  This function always uses the `Locale/US` locale."
  [^CharSequence s]
  (.. s toString (toLowerCase (java.util.Locale/US))))

(defn maybe-derive
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))
