(ns bluejdbc.util.log
  (:require [clojure.tools.logging :as log]))

(def ^:dynamic *enable-debug-logging*
  "Whether to print log messages to stdout. Useful for debugging things from the REPL."
  false)

(defmacro with-debug-logging
  "Execute `body` and print all Blue JDBC log messages to stdout. Useful for debugging things from the REPL."
  [& body]
  `(binding [*enable-debug-logging* true]
     ~@body))

(defn debug-logp
  "Print log messages to stdout for debugging. (For `logp`-style logging macros.)"
  [x & args]
  (if (instance? Throwable x)
    (do
      (println x)
      (apply println args))
    (apply println x args)))

(defn debug-logf
  "Print log messages to stdout for debugging. (For `logf`-style logging macros.)"
  [x & args]
  (if (instance? Throwable x)
    (do
      (println x)
      (println (apply format args)))
    (println (apply format x args))))

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
