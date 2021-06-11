(ns bluejdbc.result-row
  (:require [potemkin :as p]))

;; TODO -- seems a little implementation-specific... are other impls besides JDBC really going to implement this?
;; Think of a more general way to expose this functionality.
(p/defprotocol+ ResultRow
  (thunks [row]
    "Get the underlying map of `col-name->thunk` for a `Row`.")
  (with-thunks [row new-thunks]))

(extend-protocol ResultRow
  nil
  (thunks [_] nil)
  (with-thunks [_] nil)

  Object
  (thunks [_] nil))

(defn result-row? [x]
  (instance? bluejdbc.result_row.ResultRow x))
