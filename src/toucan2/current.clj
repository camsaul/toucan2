(ns toucan2.current)

;; TODO -- is this supposed to be CONNECTION or CONNECTABLE ?
;; TODO -- default connection support
;; TODO -- `:toucan/default` or just `:default`?
(def ^:dynamic *connection* :toucan/default)

(def ^:dynamic *model* nil)
