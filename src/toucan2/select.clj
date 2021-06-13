(ns toucan2.select
  "Table-aware methods for fetching data from the database. `select` and related methods."
  (:refer-clojure :exclude [count])
  (:require [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.compile :as compile]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.honeysql-util :as honeysql-util]
            [toucan2.log :as log]
            [toucan2.query :as query]
            [toucan2.queryable :as queryable]
            [toucan2.realize :as realize]
            [toucan2.specs :as specs]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

;; TODO -- consider whether this should be moved to `query`
(defn reducible-query-as
  ([tableable queryable]
   (reducible-query-as (conn.current/current-connectable tableable) tableable queryable nil))

  ([connectable tableable queryable]
   (reducible-query-as connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (query/reducible-query connectable tableable queryable options))))

(m/defmulti select*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod select* :default
  [connectable tableable query options]
  (reducible-query-as connectable tableable (compile/from connectable tableable query options) options))

(m/defmethod select* [:default :default nil]
  [connectable tableable _ options]
  (next-method connectable tableable {} options))

(m/defmethod select* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (let [query (merge {:select [:*]} query)]
    (next-method connectable tableable query options)))

(m/defmulti parse-select-args*
  {:arglists '([connectableᵈ tableableᵈ argsᵈᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(defn select-args-spec [connectable tableable]
  (letfn [(query? [x] (or (map? x) (queryable/queryable? connectable tableable x)))]
    ;; TODO -- rename these keys, since query is not necessarily a map.
    (s/cat :query   (s/alt :map     (s/cat :pk         (s/? ::specs/pk)
                                           :conditions ::specs/kv-conditions
                                           :query      (s/? query?))
                           :non-map (s/cat :query (s/? (complement query?))))
           :options (s/? ::specs/options))))

(m/defmethod parse-select-args* :default
  [connectable tableable args _]
  (let [spec   (select-args-spec connectable tableable)
        parsed (s/conform spec args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain-str spec args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (let [{[_ {:keys [pk query conditions]}] :query, :keys [options]} parsed]
      {:pk         pk
       :conditions (when (seq conditions)
                     (zipmap (map :k conditions) (map :v conditions)))
       ;; TODO -- should probably be `:queryable` instead of `:query` for clarity.
       :query      query
       :options    options})))

(m/defmulti compile-select*
  {:arglists '([connectableᵈ tableableᵈ parsed-select-argsᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod compile-select* :default
  [connectable tableable parsed-select-args options-1]
  (let [{:keys [pk conditions query options]} parsed-select-args
        options                               (u/recursive-merge options-1 options)
        conditions                            (cond-> conditions
                                                pk (honeysql-util/merge-primary-key connectable tableable pk options))
        query                                 (if query
                                                (queryable/queryable connectable tableable query options)
                                                {})
        query                                 (cond-> query
                                                (seq conditions) (honeysql-util/merge-conditions
                                                                  connectable tableable conditions options))]
    {:query query, :options options}))

;; TODO -- I think this should just take `& options` and do the `parse-connectable-tableable` stuff inside this fn.
(defn parse-select-args
  "Parse args to the `select` family of functions. Returns a map with the parsed/combined `:query` and parsed
  `:options`."
  [connectable tableable args options]
  (log/with-trace ["Parsing select args for %s %s" tableable args]
    (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
          parsed-select-args    (parse-select-args* connectable tableable args options)
          compiled              (compile-select* connectable tableable parsed-select-args options)]
      (assert (and (map? compiled) (contains? compiled :query) (contains? compiled :options))
              "compile-select* should return a map with :query and :options")
      compiled)))

(defn select-reducible
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [query options]} (parse-select-args connectable tableable args options)]
    (select* connectable tableable query options)))

(defn select
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (let [result (realize/realize (apply select-reducible args))]
    (assert (not (instance? clojure.core.Eduction result)))
    result))

(defn select-one
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (query/reduce-first (map realize/realize) (apply select-reducible args)))

(defn select-fn-reducible
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f & args]
  (eduction
   (map f)
   (apply select-reducible args)))

(defn select-fn-set
  "Like `select`, but returns a set of values of `(f instance)` for the results. Returns `nil` if the set is empty."
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-fn-reducible args))))

(defn select-fn-vec
  "Like `select`, but returns a vector of values of `(f instance)` for the results. Returns `nil` if the vector is
  empty."
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj [] (apply select-fn-reducible args))))

(defn select-one-fn
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-fn-reducible args)))

(defn select-pks-fn [connectable tableable]
  (let [pk-keys (tableable/primary-key-keys connectable tableable)]
    (if (= (clojure.core/count pk-keys) 1)
      (first pk-keys)
      (apply juxt pk-keys))))

(defn select-pks-reducible
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        f                       (select-pks-fn connectable tableable)]
    (apply select-fn-reducible f [connectable tableable] args)))

(defn select-pks-set
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-pks-reducible args))))

(defn select-pks-vec
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj [] (apply select-pks-reducible args))))

(defn select-one-pk
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-pks-reducible args)))

(defn select-fn->fn
  {:arglists '([f1 f2 connectable-tableable pk? & conditions? queryable? options?])}
  [f1 f2 & args]
  (not-empty
   (into
    {}
    (map (juxt f1 f2))
    (apply select-reducible args))))

(defn select-fn->pk
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        pks-fn                  (select-pks-fn connectable tableable)]
    (apply select-fn->fn f pks-fn [connectable tableable] args)))

(defn select-pk->fn
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        pks-fn                  (select-pks-fn connectable tableable)]
    (apply select-fn->fn pks-fn f [connectable tableable] args)))

(m/defmulti count*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod count* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable honeysql-form options]
  (query/reduce-first
   (map :count)
   (select* connectable tableable (assoc honeysql-form :select [[:%count.* :count]]) options)))

(defn count
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [query options]} (parse-select-args connectable tableable args options)]
    (count* connectable tableable query options)))

(m/defmulti exists?*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

;; TODO -- it seems like it would be a lot more efficient if we could use some bespoke JDBC code here e.g. for example
;; simply checking whether the `ResultSet` returns next (rather than fetching the row in the first place)
;;
;; At least we're avoiding the overhead of creating an actual row map since we're only fetching a single column.
(m/defmethod exists?* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable honeysql-form options]
  (boolean
   (query/reduce-first
    (map :one)
    (select* connectable tableable (assoc honeysql-form :select [[1 :one]], :limit 1) options))))

(defn exists?
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [query options]} (parse-select-args connectable tableable args options)]
    (exists?* connectable tableable query options)))
