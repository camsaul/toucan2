(ns toucan2.log
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.tools.logging :as tools.log]
   [clojure.tools.logging.impl :as tools.log.impl]
   [environ.core :as env]
   [pretty.core :as pretty]
   [puget.color]
   [puget.printer :as puget]))

(set! *warn-on-reflection* true)

(defonce all-topics (atom #{:compile :execute :results}))

(def all-levels
  {:disabled 5 ; no FATAL level because Toucan doesn't do anything fatal (hopefully). That would be zero can, not toucan
   :error    4
   :warn     3
   :info     2
   :debug    1
   :trace    0})

(defonce level (atom (some-> (env/env :toucan-debug-level) keyword)))

(def ^:dynamic *level* nil)

(defn- env-var-topics
  ([]
   (env-var-topics (env/env :toucan-debug-topics)))
  ([s]
   (when (seq s)
     (when-let [topic-strs (str/split s #",")]
       (into #{} (map keyword) topic-strs)))))

(defonce topics (atom (or (env-var-topics)
                          (constantly true))))

(def ^:dynamic *topics* nil)

(def ^:private ^:dynamic *color*
  "Whether or not to print the trace in color. True by default, unless the env var `NO_COLOR` is true."
  (if-let [env-var-value (env/env :no-color)]
    (complement (Boolean/parseBoolean env-var-value))
    true))

(defmulti print-handler
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
  (fn [printer {:keys [forms ns-symb]}]
    (let [ns-symb (when ns-symb
                    (symbol (last (str/split (name ns-symb) #"\."))))]
      [:group
       (when ns-symb
         [:span
          (puget.color/document printer :number (name ns-symb))
          [:text " "]])
       [:align
        (for [form forms]
          (puget/format-doc printer form))]])))

(defmethod print-handler Class
  [_klass]
  (fn [printer ^Class klass]
    (puget.color/document printer :class-name (.getCanonicalName klass))))

(defrecord ^:no-doc Text [s])

(defmethod print-handler Text
  [_klass]
  (fn [_printer {:keys [s]}]
    [:span
     [:text s]
     [:line]]))

(prefer-method print-handler pretty.core.PrettyPrintable clojure.lang.IRecord)

(def ^:private printer-options
  {:print-handlers print-handler
   :width          120
   :print-meta     true
   :coll-limit     5})

(defn- default-color-printer [x]
  ;; don't print in black. I can't see it
  (puget/cprint x (assoc printer-options :color-scheme {:nil [:green]})))

(defn- default-boring-printer [x]
  (puget/pprint x printer-options))

(defn- pretty-printer []
  (if *color*
    default-color-printer
    default-boring-printer))

(defn ^:no-doc pprint-doc
  "Pretty print a `doc`."
  ([doc]
   (pprint-doc nil doc))
  ([ns-symb doc]
   (try
     ((pretty-printer) (assoc doc :ns-symb ns-symb))
     (catch Throwable e
       (throw (ex-info (format "Error pretty printing doc: %s" (ex-message e))
                       {:doc doc}
                       e))))))

(defn ^:no-doc pprint-doc-to-str [doc]
  (str/trim (with-out-str (pprint-doc doc))))

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

(defn- build-doc
  "Convert `format-string` and `args` into something that can be pretty-printed by puget."
  [format-string & args]
  (let [->Text* (fn [s]
                  (list `->Text (str/trimr s)))
        texts   (map ->Text* (str/split format-string #"%s"))]
    `(->Doc ~(vec (interleave-all texts args)))))

(defn enable-level? [a-level]
  (let [current-level (or *level* @level)
        level-int     (get all-levels current-level Integer/MAX_VALUE)
        a-level-int   (get all-levels a-level 0)]
    (>= a-level-int level-int)))

(defn enable-topic? [a-topic]
  (let [current-topics (or *topics* @topics #{})]
    (current-topics a-topic)))

(defn enable-debug-message? [a-level a-topic]
  (and (enable-level? a-level)
       (enable-topic? a-topic)))

(defn enabled-logger [ns-symb a-level]
  (let [logger (tools.log.impl/get-logger tools.log/*logger-factory* ns-symb)]
    (when (tools.log.impl/enabled? logger a-level)
      logger)))

(defmacro log* [a-level a-topic e doc]
  `(let [doc# (delay ~doc)]
     (when (enable-debug-message? ~a-level ~a-topic)
       (pprint-doc '~(ns-name *ns*) @doc#))
     (when-let [logger# (enabled-logger '~(ns-name *ns*) ~a-level)]
       (tools.log/log* logger# ~a-level ~e (pprint-doc-to-str @doc#)))))

(defmacro logf [a-level a-topic & args]
  (let [[e format-string & args] (if (string? (first args))
                                   (cons nil args)
                                   args)]
    (assert (string? format-string))
    (let [doc (apply build-doc format-string args)]
      `(log* ~a-level ~a-topic ~e ~doc))))

;;; The log macros only work with `%s` for now.
(defn- correct-number-of-args-for-format-string? [{:keys [msg args]}]
  (let [matcher                 (re-matcher #"%s" msg)
        format-string-arg-count (count (take-while some? (repeatedly #(re-find matcher))))]
    (= (count args) format-string-arg-count)))

(defn- pr-str-form? [form]
  (and (seq? form)
       (= (first form) 'pr-str)))

(s/def ::args
  (s/& (s/cat :topic #(contains? @all-topics %)
              :e     (s/? (complement string?))
              :msg   string?
              :args  (s/* (complement pr-str-form?)))
       correct-number-of-args-for-format-string?))

(defmacro errorf
  [& args]
  `(logf :error ~@args))

(s/fdef errorf
  :args ::args
  :ret  any?)

(defmacro warnf
  [& args]
  `(logf :warn ~@args))

(s/fdef warnf
  :args ::args
  :ret  any?)

(defmacro infof
  "Only things that all users should see by default without configuring a logger should be this level."
  [& args]
  `(logf :info ~@args))

(s/fdef infof
  :args ::args
  :ret  any?)

(defmacro debugf
  "Most log messages should be this level."
  [& args]
  `(logf :debug ~@args))

(s/fdef debugf
  :args ::args
  :ret  any?)

(defmacro tracef
  "Log messages that are done once-per-row should be this level."
  [& args]
  `(logf :trace ~@args))

(s/fdef tracef
  :args ::args
  :ret  any?)

(doseq [varr [#'errorf #'warnf #'infof #'debugf #'tracef]]
  (alter-meta! varr assoc :arglists (list
                                     [@all-topics 'e? 'msg]
                                     [@all-topics 'e? 'format-string '& 'args])))

(comment
  (reset! topics #{:compile})
  (reset! level :debug)
  (tracef :compile "VERY NICE MESSAGE %s 1000" :abc))
