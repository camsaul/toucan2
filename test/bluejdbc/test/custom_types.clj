(ns bluejdbc.test.custom-types
  "(Incomplete) custom impls of `IInstance` and `IRow` to make sure things are flexible enough to work with types other
  than the ones provided."
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.row :as row]
            [potemkin :as p]
            [pretty.core :as pretty]))

(comment instance/keep-me row/keep-me)

(p/def-map-type CustomIInstance [orig m]
  (get [_ k default-value]
    (get m k default-value))
  (assoc [this k v]
    (CustomIInstance. orig (assoc m k v)))
  (dissoc [this k]
    (CustomIInstance. orig (dissoc m k)))
  (keys [_]
    (keys m))

  bluejdbc.instance.IInstance
  (original [_]
    orig)
  (with-original [_ new-orig]
    (CustomIInstance. m new-orig))
  (current [_]
    m)
  (with-current [_ new-current]
    (CustomIInstance. m new-current))
  (tableable [_]
    :x)

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `->CustomIInstance) m orig))

  clojure.lang.IPersistentCollection
  (equiv [_ another]
    (= m another)))

(p/def-map-type CustomIRow [m]
  (get [_ k default-value]
    (if-let [thunk (get m k)]
      (thunk)
      default-value))
  (assoc [_ k v]
    (CustomIRow. (assoc m k (constantly v))))
  (dissoc [_ k]
    (CustomIRow. (dissoc m k)))
  (keys [_]
    (keys m))

  bluejdbc.row.IRow
  (thunks [_]
    m)
  (with-thunks [_ new-thunks]
    (CustomIRow. new-thunks))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `->CustomIRow) m))

  clojure.lang.IPersistentCollection
  (equiv [_ another]
    (= m another)))
