(ns toucan2.map-backend
  "Map query build/compilation backend"
  (:require [methodical.core :as m]
            [toucan2.log :as log]))

(def default-backend (atom :toucan.map-backend/honeysql2))

(def ^:dynamic *backend-override* nil)

(m/defmulti load-backend-if-needed
  {:arglists '([map-compilation-backend₁])}
  keyword)

(m/defmethod load-backend-if-needed :toucan.map-backend/honeysql2
  [_backend]
  (when-not ((loaded-libs) 'toucan2.map-backend.honeysql2)
    (locking clojure.lang.RT/REQUIRE_LOCK
      (require 'toucan2.map-backend.honeysql2))))

(defn backend
  "Get the current map query build/compilation backend."
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
