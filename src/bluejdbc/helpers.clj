(ns bluejdbc.helpers
  (:require [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.transformed :as transformed]
            [methodical.core :as m]))

(defn dispatch-value-2 [dispatch-value]
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

(defn dispatch-value-3 [dispatch-value]
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

(defn do-helper [msg tableable thunk]
  (log/with-trace ["Doing %s for %s" msg tableable]
    (try
      (thunk)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" msg (pr-str tableable) (ex-message e))
                        {:tableable tableable}
                        e))))))

(defmacro helper {:style/indent 2} [msg tableable & body]
  `(do-helper ~msg ~tableable (fn [] ~@body)))

(defmacro define-table-name {:style/indent :defn} [dispatch-value & body]
  `(m/defmethod tableable/table-name* ~(dispatch-value-2 dispatch-value)
     [~'&connectable ~'&tableable ~'&options]
     ~@body))

(defn do-before-select
  [connectable tableable query options f]
  (helper "before-select" tableable
    (f query)))

(defmacro define-before-select {:style/indent :defn} [dispatch-value [query-binding] & body]
  `(m/defmethod select/select* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-select ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~query-binding]
                         ~@body))))

(defn do-after-select [connectable tableable reducible-query options f]
  (helper "after-select" tableable
    (eduction
     (map f)
     reducible-query)))

(defmacro define-after-select {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod select/select* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&reducible-query ~'&options]
     (do-after-select ~'&connectable ~'&tableable ~'&reducible-query ~'&options
                      (fn [~instance-binding]
                        ~@body))))

;; HELPERS BELOW ARE EXPERIMENTAL/UNTESTED

(defn do-before-update [connectable tableable query options f]
  (helper "before-update" tableable
    (update query :set (fn [values]
                         (let [pk       (get-in options [::mutative/update :pk])
                               instance (select/select-one [connectable tableable] pk)
                               updated  (merge instance values)]
                           (f updated))))))

(defmacro define-before-update {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/update!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-update ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~instance-binding]
                         ~@body))))

(defn do-after-update [connectable tableable result options f]
  (helper "after-update" tableable
    (f result)))

(defmacro define-after-update {:style/indent :defn} [dispatch-value [result-binding] & body]
  `(m/defmethod mutative/update!* :after ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&result ~'&options]
     (do-after-update ~'&connectable ~'&tableable ~'&result ~'&options
                      (fn [~result-binding]
                        ~@body))))

(defn do-before-insert [connectable tableable query options f]
  (helper "before-insert" tableable
    (update query :values (fn [rows]
                            (mapv
                             (fn [row]
                               (f (instance/instance connectable tableable row)))
                             rows)))))

(defmacro define-before-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/insert!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-insert ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~instance-binding]
                         ~@body))))

(defn do-after-insert [connectable tableable query options next-method f]
  (helper "after-insert" tableable
    (let [reducible-query (next-method connectable tableable query (-> options
                                                                       (assoc :reducible? true)
                                                                       (assoc-in [:next.jdbc :return-keys] true)))
          pks             (into [] (map (select/select-pks-fn connectable tableable)) reducible-query)
          instances       (select/select [connectable tableable]
                                         :bluejdbc/with-pks pks
                                         {}
                                         (update options :next.jdbc dissoc :return-keys))]
      (mapv f instances))))

(defmacro define-after-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/insert!* :around ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-after-insert ~'&connectable ~'&tableable ~'&query ~'&options ~'next-method
                      (fn [~instance-binding]
                        ~@body))))

(defn do-before-delete [connectable tableable query options f]
  (helper "before-delete" tableable
    (let [instances (select/select [connectable tableable] (select-keys query [:where]))]
      (doseq [instance instances]
        (f instance)))))

(defmacro define-before-delete {:style/indent :defn} [dispatch-value [instance-binding] & body]
  `(m/defmethod mutative/delete!* :before ~(dispatch-value-3 dispatch-value)
     [~'&connectable ~'&tableable ~'&query ~'&options]
     (do-before-delete ~'&connectable ~'&tableable ~'&query ~'&options
                       (fn [~instance-binding]
                         ~@body))))

#_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
  (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
    [~'&connectable ~'&tableable _ ~'&options]
    ~@body))

(defmacro define-hydration-keys
  {:style/indent 1}
  [connectable-tableable & ks]
  (let [[connectable tableable] (if (sequential? connectable-tableable)
                                  connectable-tableable
                                  [:default connectable-tableable])]
    `(do
       ~@(for [k ks]
           `(m/defmethod hydrate/table-for-automagic-hydration* [~connectable ~k]
              [~'_ ~'_]
              ~tableable)))))

(defmacro deftransforms
  {:style/indent 1}
  [dispatch-value transforms]
  `(m/defmethod transformed/transforms* ~(dispatch-value-2 dispatch-value)
     [~'&connectable ~'&tableable ~'&options]
     ~transforms))
