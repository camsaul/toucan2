(ns toucan2.map-backend
  "Map query build/compilation backend.

  A map backend must named by a keyword, and should implement the following methods:

  - [[toucan2.map-backend/load-backend-if-needed]]: initialize the backend as needed and load any namespaces that may
    need to be loaded.

  - [[toucan2.query/apply-kv-arg]]: Tell Toucan 2 what to do with key-value args to functions
    like [[toucan2.select/select]].

  - [[toucan2.pipeline/build]]: splice in default values, and handle parsed-args as passed to various functions. You may
    want separate implementations for different query types; see [[toucan2.types]] for the query type hierarchy)

  - [[toucan2.pipeline/compile]]: compile the map into a query that can be executed natively by the query execution
    backend."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.types :as types]))

(comment s/keep-me
         types/keep-me)

(defonce ^{:doc "The default map backend to use if no other backend is specified. This is stored in an atom, so you can
  `reset!` it to something else if you need to. By default, this is `:toucan.map-backend/honeysql2`."}
  default-backend
  (atom :toucan.map-backend/honeysql2))

(def ^:dynamic *backend-override*
  "You can bind this to override the [[default-backend]]."
  nil)

(m/defmulti load-backend-if-needed
  "Initialize a map backend if needed (usually this means loading some namespace with map backend method
  implementations)."
  {:arglists            '([map-compilation-backend₁])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.keyword-or-class)}
  protocols/dispatch-value)

(m/defmethod load-backend-if-needed :toucan.map-backend/honeysql2
  "Initialize the Honey SQL 2 map backend."
  [_backend]
  (when-not ((loaded-libs) 'toucan2.map-backend.honeysql2)
    (locking clojure.lang.RT/REQUIRE_LOCK
      (require 'toucan2.map-backend.honeysql2))))

(defn backend
  "Get the current map query build/compilation backend. Uses [[*backend-override*]], if bound;
  otherwise [[default-backend]]."
  []
  (let [bckend (or *backend-override*
                   @default-backend)]
    (load-backend-if-needed bckend)
    (assert (isa? bckend :toucan.map-backend/*)
            (format "Invalid map backend %s: map backends must derive from :toucan.map-backend/*"
                    (pr-str bckend)))
    (log/debugf :compile "Using map backend %s" bckend)
    bckend))

(defn available-backends
  "Return a set of all known map query build/compilation backends."
  []
  (set (keys (m/primary-methods load-backend-if-needed))))
