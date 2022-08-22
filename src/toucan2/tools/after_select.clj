(ns toucan2.tools.after-select
  (:require
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.operation :as op]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(defmacro define-after-select-reducible
  {:style/indent :defn}
  [model [reducible-query-binding] & body]
  `(m/defmethod op/reducible-returning-instances* :after [::select/select ~model]
     [~'&query-type ~'&model ~reducible-query-binding]
     ~@body))

(defn do-after-select-each [model reducible-query f]
  (eduction (map (fn [instance]
                   (u/with-debug-result ["do after-select for %s %s" model instance]
                     (try
                       (instance/reset-original (f instance))
                       (catch Throwable e
                         (throw (ex-info (format "Error in %s for %s: %s"
                                                 `define-after-select-each (u/safe-pr-str model) (ex-message e))
                                         {:model model, :instance instance}
                                         e)))))))
            reducible-query))

(defmacro define-after-select-each
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(define-after-select-reducible ~model [reducible-query#]
     (do-after-select-each ~'&model reducible-query# (fn [~instance-binding] ~@body))))
