(ns toucan2.log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pretty.core :as pretty]
            [puget.printer :as puget]))

(defn pprint [x]
  ;; don't print in black. I can't see it
  (binding [puget/*options* (assoc-in puget/*options* [:color-scheme :nil] nil)
            *print-meta*    true
            *print-length*  5]
    (let [x (if (instance? pretty.core.PrettyPrintable x)
              (with-meta (pretty/pretty x) (meta x))
              x)]
      (puget/cprint x))))

(def ^:dynamic *enable-debug-logging*
  "Whether to print log messages to stdout. Useful for debugging things from the REPL."
  false)

(defn maybe-doall
  "Fully realize `result` if it sequential (i.e. a lazy seq) and debug logging is enabled. This is done so various
  operations like `for` and `map` get logged correctly when debugging stuff."
  [result]

  (cond-> result
    (and *enable-debug-logging*
         (seqable? result)
         ;; Eduction is seqable, but we don't want to fully realize it!
         (not (instance? clojure.core.Eduction result)))
    doall))

;; TODO -- with-debug-logging should enable `*include-queries-in-exceptions*` and
;; `*include-connection-info-in-exceptions*` as well
(defmacro with-debug-logging
  "Execute `body` and print all Toucan 2 log messages to stdout. Useful for debugging things from the REPL."
  [& body]
  `(binding [*enable-debug-logging* true]
     (maybe-doall (do ~@body))))

(defn- maybe-pr-str [x]
  (cond
    (string? x) x
    (number? x) x
    (class? x)  (symbol (.getCanonicalName ^Class x))
    :else       (-> (binding [*print-meta* true] (with-out-str (pprint x)))
                    (str/replace #"\s*\n\s*" " ")
                    str/trim)))

(defn- log* [f x args]
  (try
    (if (instance? Throwable x)
      (do
        (pprint (Throwable->map x))
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

(defn do-indent-when-debugging
  [thunk]
  (if *enable-debug-logging*
    (binding [*indent-level* (inc *indent-level*)]
      (thunk))
    (thunk)))

(defmacro indent-when-debugging {:style/indent 0} [& body]
  `(do-indent-when-debugging (fn [] ~@body)))

(defn pprint-result-to-str [result]
  ;; don't try to pretty-print Eductions: it never ends well
  (if (instance? clojure.core.Eduction result)
    (str "-> " result)
    (try
      (with-out-str
        (let [lines               (-> result pprint with-out-str str/trim str/split-lines)
              [first-line & more] lines]
          (println (str "=> " first-line))
          (doseq [line more]
            (println (str "   " line)))))
      (catch Throwable e
        (throw (ex-info (format "Error pretty-printing %s result: %s" (some-> result class (.getCanonicalName)) (ex-message e))
                        {:result result}
                        e))))))

(defmacro with-trace-no-result [message & body]
  `(do
     ~(if (vector? message)
        `(tracef ~@message)
        `(trace ~message))
     (indent-when-debugging
       (maybe-doall (do ~@body)))))

(defmacro with-trace [message & body]
  `(do
     ~(if (vector? message)
        `(tracef ~@message)
        `(trace ~message))
     (indent-when-debugging
       (let [result# (maybe-doall (do ~@body))]
         (trace (pprint-result-to-str result#))
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
