(ns bluejdbc.connectable.current)

;; The only reason these are in their own namespace is to prevent circular refs.

(def ^:dynamic *current-connectable* :bluejdbc/default)

(def ^:dynamic ^java.sql.Connection *current-connection* nil)
