(ns bluejdbc.log
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(def ^:dynamic *enable-debug-logging*
  "Whether to print log messages to stdout. Useful for debugging things from the REPL."
  false)

;; TODO -- with-debug-logging should enable `*include-queries-in-exceptions*` and
;; `*include-connection-info-in-exceptions*` as well
(defmacro with-debug-logging
  "Execute `body` and print all Blue JDBC log messages to stdout. Useful for debugging things from the REPL."
  [& body]
  `(binding [*enable-debug-logging* true]
     ~@body))

(defn- maybe-pr-str [x]
  (cond
    (string? x) x
    (number? x) x
    :else       (pr-str x)))

(defn- log* [f x args]
  (try
    (if (instance? Throwable x)
      (do
        (pprint/pprint (Throwable->map x))
        (f args))
      (f (cons x args)))
    (catch Throwable e
      (throw (ex-info (str "Error printing log message: " (ex-message e)) {:args args} e)))))

(def ^:dynamic *indent-level* 0)

(defn- indent-println [s]
  (doseq [line (str/split-lines (str/trim (with-out-str (println s))))]
    (dotimes [_ *indent-level*]
      (print "  "))
    (println line)))

(defn debug-logp
  "Print log messages to stdout for debugging. (For `logp`-style logging macros.)"
  [x & args]
  (log*
   (fn [args]
     (apply indent-println (map maybe-pr-str args)))
   x
   args))

(defn debug-logf
  "Print log messages to stdout for debugging. (For `logf`-style logging macros.)"
  [x & args]
  (log*
   (fn [[format-string & args]]
     (indent-println (apply format format-string (map maybe-pr-str args))))
   x
   args))

(defmacro logp
  "Like `clojure.tools.logging/logp`, but also prints log messages to stdout if debug logging is enabled."
  [level x & more]
  `(do
     (when *enable-debug-logging*
       (debug-logp ~x ~@more))
     (log/logp ~level ~x ~@more)))

(defmacro logf
  "Like `clojure.tools.logging/logf`, but also prints log messages to stdout if debug logging is enabled."
  [level x & more]
  `(do
     (when *enable-debug-logging*
       (debug-logf ~x ~@more))
     (log/logf ~level ~x ~@more)))

(defmacro trace
  "Like `clojure.tools.logging/trace`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logp :trace ~@args))

(defmacro tracef
  "Like `clojure.tools.logging/tracef`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logf :trace ~@args))

(defmacro with-trace [message & body]
  `(do
     ~(if (vector? message)
        `(tracef ~@message)
        `(trace ~message))
     (binding [*indent-level* (inc *indent-level*)]
       (let [result# (do ~@body)]
         (tracef "-> %s" result#)
         result#))))

(defmacro debug
  "Like `clojure.tools.logging/debug`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logp :debug ~@args))

(defmacro debugf
  "Like `clojure.tools.logging/debugf`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logf :debug ~@args))

(defmacro warn
  "Like `clojure.tools.logging/warn`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logp :warn ~@args))

(defmacro warnf
  "Like `clojure.tools.logging/warnf`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logf :warn ~@args))

(defmacro error
  "Like `clojure.tools.logging/error`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logp :error ~@args))

(defmacro errorf
  "Like `clojure.tools.logging/errorf`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logf :error ~@args))

(defmacro fatal
  "Like `clojure.tools.logging/fatal`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logp :fatal ~@args))

(defmacro fatalf
  "Like `clojure.tools.logging/fatalf`, but also prints log messages to stdout if debug logging is enabled."
  [& args]
  `(logf :fatal ~@args))
