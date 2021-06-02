(ns bluejdbc.helpers
  (:require [bluejdbc.select :as select]
            [methodical.core :as m]))

(defn- dispatch-value-3 [dispatch-value]
  (let [{:keys [connectable tableable query-class]}
        (if-not (sequential? dispatch-value)
          {:tableable dispatch-value}
          (zipmap
           (condp = (count dispatch-value)
             1 [:tableable]
             2 [:connectable :tableable]
             [:connectable :tableable :query-class])
           dispatch-value))]
    [(or connectable :default)
     (or tableable :default)
     (or query-class :default)]))

(defmacro define-before-select {:style/indent :defn} [dispatch-value [query-binding] & body]
  `(m/defmethod select/select* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~query-binding ~'&options]
     ~@body))

(defmacro define-after-select {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod select/select* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&reducible-query ~'&options]
     (eduction
      (map (fn [~instance-binding]
             ~@body))
      ~'&reducible-query)))
