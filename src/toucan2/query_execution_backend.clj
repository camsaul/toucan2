(ns toucan2.query-execution-backend
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.protocols :as protocols]
   [toucan2.types :as types]))

(comment s/keep-me
         types/keep-me)

(m/defmulti load-backend-if-needed
  "Initialize a query execution backend if needed (usually this means loading some namespace with map backend method
  implementations)."
  {:arglists            '([connection₁])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.keyword-or-class)}
  protocols/dispatch-value)

(m/defmethod load-backend-if-needed :default
  "Default method: no-op."
  [_connection]
  nil)

(m/defmethod load-backend-if-needed java.sql.Connection
  "Load the [[toucan2.query-execution-backend.jdbc]] implementation."
  [_connection]
  (when-not ((loaded-libs) 'toucan2.query-execution-backend.jdbc)
    (locking clojure.lang.RT/REQUIRE_LOCK
      (require 'toucan2.query-execution-backend.jdbc))))
