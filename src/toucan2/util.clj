(ns toucan2.util
  (:require [clojure.string :as str]))

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

(defn keyword-or-type [x]
  (if (keyword? x)
    x
    (type x)))

;; TODO (maybe) `u/dispatch-value`

;; TODO -- rename to `dispatch-on-first-arg` or `dispatch-on-keyword-or-type-of-first-arg`
(defn dispatch-on-keyword-or-type-1
  [x & _]
  (keyword-or-type x))

;; TODO -- rename to `u/dispatch-on-first-two-args`
(defn dispatch-on-keyword-or-type-2
  [x y & _]
  [(keyword-or-type x)
   (keyword-or-type y)])

(defn lower-case-en
  "Locale-agnostic version of [[clojure.string/lower-case]]. `clojure.string/lower-case` uses the default locale in conversions, turning `ID` into `ıd`, in the Turkish locale.
  This function always uses the `Locale/US` locale."
  [^CharSequence s]
  (.. s toString (toLowerCase (java.util.Locale/US))))
