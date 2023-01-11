(ns toucan2.test.track-realized-columns
  "A version of the `venues` model that tracks which columns are realized."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(def ^:dynamic ^:private *realized-columns* nil)

(doto ::venues
  (derive ::test/venues)
  (derive ::track-realized))

(doto ::people
  (derive ::test/people)
  (derive ::track-realized))

(m/defmethod jdbc.read/read-column-thunk :before [#_conn :default #_model ::track-realized #_col-type :default]
  [_conn _model _rset ^java.sql.ResultSetMetaData rsmeta ^Integer i]
  (when *realized-columns*
    (let [table-name (csk/->kebab-case (.getTableName rsmeta i))
          col-name   (csk/->kebab-case (.getColumnName rsmeta i))]
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
