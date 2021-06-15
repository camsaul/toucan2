(ns toucan2.jdbc
  "Convenience namespace for loading the whole JDBC backend."
  (:require toucan2.jdbc.query
            toucan2.jdbc.result-set
            toucan2.jdbc.row
            toucan2.jdbc.statement
            [methodical.core :as m]
            [toucan2.queryable :as queryable]))

(comment
  toucan2.jdbc.query/keep-me
  toucan2.jdbc.result-set/keep-me
  toucan2.jdbc.row/keep-me
  toucan2.jdbc.statement/keep-me)
