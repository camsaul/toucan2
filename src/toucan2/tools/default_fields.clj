(ns toucan2.tools.default-fields
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti default-fields
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod pipeline/transduce-with-model :before [#_query-type :toucan.result-type/instances
                                                    #_model      ::default-fields]
  [_rf _query-type model parsed-args]
  (log/debugf :compile "add default fields for %s" model)
  (let [result (update parsed-args :columns (fn [columns]
                                              (or (not-empty columns)
                                                  (default-fields model))))]
    (log/debugf :compile "[add default fields] => %s" result)
    result))

(defmacro define-default-fields {:style/indent :defn} [model & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))

(s/fdef define-default-fields
  :args (s/cat :model some?
               :body  (s/+ any?))
  :ret any?)
