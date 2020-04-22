(ns bluejdbc.protocols
  (:require [potemkin.types :as p.types]))

;; TODO -- this should be moved to options namespace
(p.types/defprotocol+ BlueJDBCProxy
  (options [this])
  (with-options [this new-options]))

(extend-protocol BlueJDBCProxy
  Object
  (options [_] nil)

  nil
  (options [_] nil))
