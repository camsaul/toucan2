(ns toucan2.test.track-realized-columns
  "A version of the `venues` model that tracks which columns are realized."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.magic-map :as magic-map]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(def ^:dynamic ^:private *realized-columns* nil)

(derive ::venues ::test/venues)

(m/defmethod jdbc.read/read-column-thunk :before [#_conn :default #_model ::venues :default]
  [_conn _model _rset ^java.sql.ResultSetMetaData rsmeta ^Integer i]
  (when *realized-columns*
    (let [table-name (magic-map/->kebab-case-string (.getTableName rsmeta i))
          col-name   (magic-map/->kebab-case-string (.getColumnName rsmeta i))]
      (swap! *realized-columns* conj (keyword table-name col-name))))
  i)

(defn do-with-realized-columns [f]
  (binding [*realized-columns* (atom #{})]
    (f (fn []
         @*realized-columns*))))

(defmacro with-realized-columns [[realized-columns-fn-binding] & body]
  `(do-with-realized-columns
    (^:once fn* [~realized-columns-fn-binding] ~@body)))

(s/fdef with-realized-columns
  :args (s/cat :bindings (s/spec (s/cat :realized-cols-fn symbol?))
               :body     (s/+ any?))
  :ret any?)
