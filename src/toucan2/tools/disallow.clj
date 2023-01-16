(ns toucan2.tools.disallow
  (:require
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/select.*
                             #_model          ::select
                             #_resolved-query :default]
  "Throw an Exception when trying to build a SELECT query for models deriving from `:toucan2.tools.disallow/select`."
  [_query-type model _parsed-args _resolved-query]
  (throw (UnsupportedOperationException. (format "You cannot select %s." model))))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/delete.*
                             #_model          ::delete
                             #_resolved-query :default]
  "Throw an Exception when trying to build a DELETE query for models deriving from `:toucan2.tools.disallow/delete`."
  [_query-type model _parsed-args _resolved-query]
  (throw (UnsupportedOperationException. (format "You cannot delete instances of %s." model))))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/insert.*
                             #_model          ::insert
                             #_resolved-query :default]
  "Throw an Exception when trying to build a INSERT query for models deriving from `:toucan2.tools.disallow/insert`."
  [_query-type model _parsed-args _resolved-query]
  (throw (UnsupportedOperationException. (format "You cannot create new instances of %s." model))))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/update.*
                             #_model          ::update
                             #_resolved-query :default]
  "Throw an Exception when trying to build a UPDATE query for models deriving from `:toucan2.tools.disallow/update`."
  [_query-type model _parsed-args _resolved-query]
  (throw (UnsupportedOperationException. (format "You cannot update a %s after it has been created." model))))
