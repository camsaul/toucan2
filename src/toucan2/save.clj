(ns toucan2.save)

;; (m/defmulti save!*
;;   {:arglists '([connectableᵈ tableableᵈ objᵈᵗ options])}
;;   u/dispatch-on-first-three-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (m/defmethod save!* :default
;;   [connectable tableable obj _]
;;   (log/with-trace ["Saving %s (changes: %s)" obj (instance/changes obj)]
;;     (if-let [changes (not-empty (instance/changes obj))]
;;       (let [pk-values     (tableable/primary-key-values connectable tableable obj)
;;             rows-affected (update! [connectable tableable] pk-values changes)]
;;         (when-not (pos? rows-affected)
;;           (throw (ex-info (format "Unable to save object: %s with primary key %s does not exist." tableable pk-values)
;;                           {:object obj
;;                            :pk     pk-values})))
;;         (when (> rows-affected 1)
;;           (log/warnf "Warning: more than 1 row affected when saving %s with primary key %s" tableable pk-values))
;;         (instance/reset-original obj))
;;       (do
;;         (log/tracef "No changes; nothing to save.")
;;         obj))))

;; (defn save!
;;   [obj]
;;   (let [tableable             (instance/tableable obj)
;;         connectable           (instance/connectable obj)
;;         [connectable options] (conn.current/ensure-connectable connectable tableable nil)]
;;     (save!* connectable tableable obj options)))
