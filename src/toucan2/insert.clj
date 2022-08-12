(ns toucan2.insert)

;; (m/defmulti insert!*
;;   "Returns the number of rows inserted."
;;   {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
;;   u/dispatch-on-first-three-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (m/defmethod insert!* :default
;;   [connectable tableable query options]
;;   (query/execute! connectable tableable query options))

;; (m/defmulti parse-insert!-args*
;;   {:arglists '([connectableᵈ tableableᵈ argsᵗ options])}
;;   u/dispatch-on-first-two-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (s/def ::insert!-args
;;   (s/cat :rows (s/alt :single-row-map    map?
;;                       :multiple-row-maps (s/coll-of map?)
;;                       :kv-pairs          ::specs/kv-conditions
;;                       :columns-rows      (s/cat :columns (s/coll-of keyword?)
;;                                                 :rows    (s/coll-of vector?)))
;;          :options (s/? ::specs/options)))

;; (m/defmethod parse-insert!-args* :default
;;   [_ _ args _]
;;   (let [parsed (s/conform ::insert!-args args)]
;;     (when (= parsed :clojure.spec.alpha/invalid)
;;       (throw (ex-info (format "Don't know how to interpret insert! args: %s" (s/explain-str ::insert!-args args))
;;                       {:args args})))
;;     (log/tracef "-> %s" (u/pprint-to-str parsed))
;;     (update parsed :rows (fn [[rows-type x]]
;;                            (condp = rows-type
;;                              :single-row-map    [x]
;;                              :multiple-row-maps x
;;                              :kv-pairs          [(into {} (map (juxt :k :v)) x)]
;;                              :columns-rows      (let [{:keys [columns rows]} x]
;;                                                   (for [row rows]
;;                                                     (zipmap columns row))))))))

;; (defn parse-insert-args [connectable-tableable args]
;;   (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
;;         [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
;;         {:keys [rows options]}  (parse-insert!-args* connectable tableable args options)
;;         query                   (when (seq rows)
;;                                   (-> (build-query/maybe-buildable-query connectable tableable nil :insert options)
;;                                       (build-query/with-table* tableable options)
;;                                       (build-query/with-rows* rows options)))]
;;     {:connectable connectable
;;      :tableable   tableable
;;      :query       query
;;      :options     options}))

;; (defn do-insert! [connectable tableable {:keys [values], :as query} options]
;;   (log/with-trace ["INSERT %d %s rows:\n%s" (count values) tableable (u/pprint-to-str values)]
;;     (try
;;       (assert (seq values) "Values cannot be empty")
;;       (insert!* connectable tableable query options)
;;       (catch Throwable e
;;         (throw (ex-info (format "Error in insert!: %s" (ex-message e))
;;                         {:tableable tableable, :query query, :options options}
;;                         e))))))

;; (defn insert!
;;   "Returns number of rows inserted."
;;   {:arglists '([connectable-tableable row-or-rows options?]
;;                [connectable-tableable k v & more options?]
;;                [connectable-tableable columns row-vectors options?])}
;;   [connectable-tableable & args]
;;   (let [{:keys [connectable tableable query options]} (parse-insert-args connectable-tableable args)]
;;     (if (empty? query)
;;       (do
;;         (log/trace "No rows to insert.")
;;         0)
;;       (do-insert! connectable tableable query options))))

;; (defn insert-returning-keys!
;;   {:arglists '([connectable-tableable row-or-rows options?]
;;                [connectable-tableable k v & more options?]
;;                [connectable-tableable columns row-vectors options?])}
;;   [connectable-tableable & args]
;;   (let [{:keys [connectable tableable query options]} (parse-insert-args connectable-tableable args)
;;         options                                       (-> options
;;                                                           ;; TODO -- this is next.jdbc specific
;;                                                           (assoc-in [:next.jdbc :return-keys] true)
;;                                                           (assoc :reducible? true))
;;         reducible-query (do-insert! connectable tableable query options)]
;;     (into
;;      []
;;      (map (select/select-pks-fn connectable tableable))
;;      reducible-query)))
