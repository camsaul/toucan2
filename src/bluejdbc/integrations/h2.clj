(ns bluejdbc.integrations.h2
  (:require [bluejdbc.result-set :as rs]
            [methodical.core :as m])
  (:import [java.sql ResultSet ResultSetMetaData Types]))

(m/defmethod rs/read-column-thunk [:h2 Types/CLOB]
  [^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i _]
  (fn []
    (.getString rs i)))
