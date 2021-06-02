(ns bluejdbc.helpers
  (:require [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [methodical.core :as m]))

(defn- dispatch-value-2 [dispatch-value]
  (let [{:keys [connectable tableable]}
        (if-not (sequential? dispatch-value)
          {:tableable dispatch-value}
          (zipmap
           (condp = (count dispatch-value)
             1 [:tableable]
             [:connectable :tableable])
           dispatch-value))]
    [(or connectable :default)
     (or tableable :default)]))

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

(defmacro define-table-name {:style/indent :defn} [dispatch-value & body]
  `(m/defmethod tableable/table-name* ~(dispatch-value-2 dispatch-value)
     [~'&connectable ~'&tableable ~'&options]
     ~@body))

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

;; HELPERS BELOW ARE EXPERIMENTAL/UNTESTED

(defmacro define-before-update {:style/indent :defn} [dispatch-value [query-binding] & body]
  `(m/defmethod mutative/update!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~query-binding ~'&options]
     ~@body))

(defmacro define-after-update {:style/indent :defn} [dispatch-value [result-binding] & body]
  `(m/defmethod mutative/update!* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~result-binding ~'&options]
     ~@body))

(defmacro define-before-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/insert!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (update ~'&query :values (fn [~'&values]
                                (map
                                 (fn [~instance-binding]
                                   ~@body)
                                 ~'&values)))))

(defmacro define-after-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(let [[~'_ tableable# :as dv#] (dispatch-value-3 ~dispatch-value)]
     (assert (isa? tableable# :bluejdbc/insert-return-instances)
             (format "%s must derive from :bluejdbc/insert-return-instances to use the define-after-insert helper" tableable#))
     (m/defmethod mutative/insert!* :after dv#
       [~'&connectable ~'&tableable instances# ~'&options]
       (for [~instance-binding instances#]
         ~@body))))

(defmacro define-before-delete {:style/indent :defn} [dispatch-value [query-binding] & body]
  `(m/defmethod mutative/delete!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~query-binding ~'&options]
     ~@body))

#_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
  (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
    [~'&connectable ~'&tableable _ ~'&options]
    ~@body))
