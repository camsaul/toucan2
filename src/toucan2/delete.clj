(ns toucan2.delete)

;; (m/defmulti parse-delete-args*
;;   {:arglists '([connectableᵈ tableableᵈ argsᵈᵗ options])}
;;   u/dispatch-on-first-two-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (m/defmethod parse-delete-args* :default
;;   [connectable tableable args options]
;;   (select/parse-select-args* connectable tableable args options))

;; (m/defmethod parse-delete-args* :around :default
;;   [connectable tableable args options]
;;   (log/with-trace ["Parsing delete! args for %s %s" tableable args]
;;     (next-method connectable tableable args options)))

;; (defn parse-delete-args [[connectable-tableable & args]]
;;   (let [[connectable tableable]            (conn/parse-connectable-tableable connectable-tableable)
;;         [connectable options-1]            (conn.current/ensure-connectable connectable tableable nil)
;;         {:keys [conditions query options]} (parse-delete-args* connectable tableable args options-1)
;;         options                            (u/recursive-merge options-1 options)
;;         query                              (cond-> (build-query/maybe-buildable-query connectable tableable query :delete options)
;;                                              true             (build-query/with-table* tableable options)
;;                                              (seq conditions) (build-query/merge-kv-conditions* conditions options))]
;;     {:connectable connectable
;;      :tableable   tableable
;;      :query       query
;;      :options     options}))

;; (m/defmulti delete!*
;;   {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
;;   u/dispatch-on-first-three-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (m/defmethod delete!* :default
;;   [connectable tableable query options]
;;   (let [query (build-query/maybe-buildable-query connectable tableable query :delete options)]
;;     (log/with-trace ["DELETE rows: %s" query]
;;       (query/execute! connectable tableable query options))))

;; (defn delete!
;;   {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
;;   [& args]
;;   (let [{:keys [connectable tableable query options]} (parse-delete-args args)]
;;     (delete!* connectable tableable query options)))
