(ns bluejdbc.util.log
  (:require [clojure.tools.logging :as log]))

(def ^:dynamic *enable-debug-logging* false)

(defmacro with-debug-logging [& body]
  `(binding [*enable-debug-logging* true]
     ~@body))

(defn debug-logp [x & args]
  (if (instance? Throwable x)
    (do
      (println x)
      (apply println args))
    (apply println x args)))

(defn debug-logf [x & args]
  (if (instance? Throwable x)
    (do
      (println x)
      (println (apply format args)))
    (println (apply format x args))))

(defmacro logp [level x & more]
  `(do
     (when *enable-debug-logging*
       (debug-logp ~x ~@more))
     (log/logp ~level ~x ~@more)))

(defmacro logf [level x & more]
  `(do
     (when *enable-debug-logging*
       (debug-logf ~x ~@more))
     (log/logf ~level ~x ~@more)))

(defmacro trace [& args]
  `(logp :trace ~@args))

(defmacro tracef [& args]
  `(logf :trace ~@args))

(defmacro debug [& args]
  `(logp :debug ~@args))

(defmacro debugf [& args]
  `(logf :debug ~@args))

(defmacro warn [& args]
  `(logp :warn ~@args))

(defmacro warnf [& args]
  `(logf :warn ~@args))

(defmacro error [& args]
  `(logp :error ~@args))

(defmacro errorf [& args]
  `(logf :error ~@args))

(defmacro fatal [& args]
  `(logp :fatal ~@args))

(defmacro fatalf [& args]
  `(logf :fatal ~@args))
