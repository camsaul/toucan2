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

(p/defprotocol+ IRow
  (thunks [row]
    "Get the underlying map of `col-name->thunk` for a `Row`.")
  (with-thunks [row new-thunks]))

(extend-protocol IRow
  nil
  (thunks [_] nil)
  (with-thunks [_] nil)

  Object
  (thunks [_] nil))

;; TODO -- maybe give this a better name like `ResultSetRow` that conveys the fact that it's tied to a result set.
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

  IRow
  (thunks [_]
    col-name->thunk)
  (with-thunks [_ new-thunks]
    (Row. new-thunks mta))

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
                   [k (fn cached-thunk [] @dlay)]))
        nil))

(defn row? [x]
  (instance? IRow x))
