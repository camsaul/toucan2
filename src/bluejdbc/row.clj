(ns bluejdbc.row
  (:require [bluejdbc.log :as log]
            [potemkin :as p]
            [pretty.core :as pretty]))

(p/defprotocol+ RealizeRow
  (realize-row [row]))

(extend-protocol RealizeRow
  Object
  (realize-row [this]
    this)

  nil
  (realize-row [_]
    nil))

(p/def-map-type Row [col-name->thunk mta]
  (get [_ k default-value]
    (if-let [thunk (get col-name->thunk k)]
      (thunk)
      default-value))

  (assoc [_ k v]
    (Row. (assoc col-name->thunk k (constantly v)) mta))

  (dissoc [_ k]
    (Row. (dissoc col-name->thunk k) mta))

  (keys [_]
    (keys col-name->thunk))

  (meta [_]
    mta)

  (with-meta [_ new-meta]
    (Row. col-name->thunk new-meta))

  RealizeRow
  (realize-row [_]
    (log/with-trace "Realize entire row"
      (-> (into {} (for [[col-name thunk] col-name->thunk]
                     [col-name (thunk)]))
          (with-meta mta))))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `row) col-name->thunk)))

(defn row [col-name->thunk]
  ;; wrap the thunks in delays so we don't end up invoking them multiple times, since they are potentially expensive.
  (Row. (into {} (for [[k thunk] col-name->thunk
                       :let      [dlay (delay (thunk))]]
                   [k (fn [] @dlay)]))
        nil))
