(ns toucan2.tools.default-fields
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti default-fields
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod query/build :before [::select/select ::default-fields clojure.lang.IPersistentMap]
  [_query-type model args]
  (u/with-debug-result ["add default fields for %s" model]
    (update args :columns (fn [columns]
                            (or (not-empty columns)
                                (default-fields model))))))

(defmacro define-default-fields {:style/indent :defn} [model & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))

(s/fdef define-default-fields
  :args (s/cat :model some?
               :body  (s/+ any?))
  :ret any?)
