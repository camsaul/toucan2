(ns toucan2.util
  (:require [clojure.string :as str]
            [potemkin :as p]))

(def ^:dynamic *debug* false)

(def ^:dynamic *debug-indent-level* 0)

(defn println-debug* [s]
  (let [lines       (str/split-lines (str/trim s))
        indentation (str/join (repeat *debug-indent-level* "|  "))]
    (doseq [line lines]
      (print indentation)
      (println line))))

(defmacro println-debug [& args]
  `(when *debug*
     (println-debug* (with-out-str (println ~@args)))))

(defn print-debug-result [result]
  (println-debug "+->" (pr-str result)))

(defn do-with-debug-result [message thunk]
  (println-debug message)
  (let [result (binding [*debug-indent-level* (inc *debug-indent-level*)]
                 (thunk))]
    (print-debug-result result)
    result))

(defmacro with-debug-result [message & body]
  `(let [thunk# (^:once fn* [] ~@body)]
     (if-not *debug*
       (thunk#)
       (do-with-debug-result ~message thunk#))))

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

(defn dispatch-on-first-two-args
  [x y & _]
  [(dispatch-value x) (dispatch-value y)])

(defn lower-case-en
  "Locale-agnostic version of [[clojure.string/lower-case]]. `clojure.string/lower-case` uses the default locale in conversions, turning `ID` into `ıd`, in the Turkish locale.
  This function always uses the `Locale/US` locale."
  [^CharSequence s]
  (.. s toString (toLowerCase (java.util.Locale/US))))
