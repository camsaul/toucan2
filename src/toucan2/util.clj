(ns toucan2.util
  (:require [clojure.string :as str]))

(def ^:dynamic *debug* false)

(def ^:dynamic ^:private *recursive-debug-result-level* 0)

(defn println-debug* [s]
  (let [lines       (str/split-lines (str/trim s))
        indentation (str/join (repeat *recursive-debug-result-level* "  "))]
    (doseq [line lines]
      (print indentation)
      (println line))))

(defmacro println-debug [& args]
  `(when *debug*
     (println-debug* (with-out-str (println ~@args)))))

(defn do-with-debug-result [message thunk]
  (println-debug message)
  (let [result (binding [*recursive-debug-result-level* (inc *recursive-debug-result-level*)]
                 (thunk))]
    (println-debug '=> (pr-str result))
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

(defn dispatch-on-keyword-or-type-1
  [x & _]
  (keyword-or-type x))

(defn dispatch-on-keyword-or-type-2
  [x y & _]
  [(keyword-or-type x)
   (keyword-or-type y)])
