(ns toucan2.log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pretty.core :as pretty]
            [puget.printer :as puget]))

(defn pprint [x]
  ;; TODO -- consider only printing `:type` in metadata, and ignoring other boring stuff.
  ;;
  ;; don't print in black. I can't see it
  (binding [puget/*options* (-> puget/*options*
                                (assoc-in [:color-scheme :nil] nil)
                                (assoc :width 120))
            *print-meta*    (:type (meta x))
            *print-length*  5]
    (let [x (if (instance? pretty.core.PrettyPrintable x)
              (with-meta (pretty/pretty x) (meta x))
              x)]
      (puget/cprint x))))

(def ^:dynamic *debug-log-level*
  "Level of logging to print to stdout. Useful for debugging things from the REPL."
  nil)

(defn debug-log? [message-level]
  (when *debug-log-level*
    (let [levels        {:trace 5
                         :debug 4
                         :info  3
                         :warn  2
                         :error 1
                         :fatal 0}
          debug-level   (get levels *debug-log-level* -1)
          message-level (get levels message-level 5)]
      (>= debug-level message-level))))

(defn maybe-doall
  "Fully realize `result` if it sequential (i.e. a lazy seq) and debug logging is enabled. This is done so various
  operations like `for` and `map` get logged correctly when debugging stuff."
  [result]
  (cond-> result
    (and *debug-log-level*
         (seqable? result)
         ;; Eduction is seqable, but we don't want to fully realize it!
         (not (instance? clojure.core.Eduction result)))
    doall))

;; TODO -- with-debug-logging should enable `*include-queries-in-exceptions*` and
;; `*include-connection-info-in-exceptions*` as well
(defmacro with-debug-logging
  "Execute `body` and print high-level Toucan 2 log messages to stdout. Useful for debugging things from the REPL."
  [& body]
  `(binding [*debug-log-level* :debug]
     (maybe-doall (do ~@body))))

(defmacro with-trace-logging
  "Execute `body` and print *all* Toucan 2 log messages to stdout. Useful for debugging things from the REPL."
  [& body]
  `(binding [*debug-log-level* :trace]
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
     (when (debug-log? ~level)
       (debug-logp ~x ~@more))
     (log/logp ~level ~x ~@more)))

(defmacro logf
  "Like `clojure.tools.logging/logf`, but also prints log messages to stdout if debug logging is enabled."
  [level x & more]
  `(do
     (when (debug-log? ~level)
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
  (if *debug-log-level*
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

(defmacro maybe-logf [level message]
  (if (vector? message)
    `(logf ~level ~@message)
    `(logp ~level ~message)))

(defmacro with-level-no-result {:style/indent 2} [level message & body]
  `(do
     (maybe-logf ~level ~message)
     (indent-when-debugging
       (maybe-doall (do ~@body)))))

(defmacro with-debug-no-result {:style/indent 1} [message & body]
  `(with-level-no-result :debug ~message ~@body))

(defmacro with-trace-no-result {:style/indent 1} [message & body]
  `(with-level-no-result :trace ~message ~@body))

(defmacro with-level {:style/indent 2} [level message & body]
  `(do
     (maybe-logf ~level ~message)
     (indent-when-debugging
       (let [result# (maybe-doall (do ~@body))]
         (logp ~level (pprint-result-to-str result#))
         result#))))

(defmacro with-debug {:style/indent 1} [message & body]
  `(with-level :debug ~message ~@body))

(defmacro with-trace {:style/indent 1} [message & body]
  `(with-level :trace ~message ~@body))

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
