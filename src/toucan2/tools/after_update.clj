(ns toucan2.tools.after-update
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.operation :as op]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(m/defmulti after-update
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod after-update :around :default
  [model instance]
  (u/with-debug-result [(list `after-update model instance)]
    (try
      (next-method model instance)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `after-update (pr-str model) (ex-message e))
                        {:model model, :row instance}
                        e))))))

(defrecord ReducibleAfterUpdate [model reducible-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (u/with-debug-result ["reducing after-update for %s" model]
      (try
        (let [affected-pks (realize/realize reducible-pks)]
          (u/println-debug ["Doing after-update for %s with PKs %s" model affected-pks])
          (reduce
           rf
           init
           (eduction
            (map (fn [row]
                   (after-update model row)
                   row))
            (select/select-reducible-with-pks model affected-pks))))
        (catch Throwable e
          (throw (ex-info (format "Error doing after update for %s: %s" (pr-str model) (ex-message e))
                          {:model model, :parent-update reducible-pks}
                          e))))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleAfterUpdate model reducible-pks)))

(m/defmethod op/reducible* :around [::update/update ::after-update]
  [query-type model {::keys [doing-after-update?], :as parsed-args}]
  (if doing-after-update?
    (next-method query-type model parsed-args)
    (u/with-debug-result ["do after-update for %s in %s" model `update/reducible-update*]
      (let [parsed-args     (assoc parsed-args ::doing-after-update? true)
            reducible-count (next-method query-type model parsed-args)
            reducible-pks   (select/return-pks-eduction model reducible-count)]
        (eduction
         (map (constantly 1))
         (->ReducibleAfterUpdate model reducible-pks))))))

(m/defmethod op/reducible-returning-pks* :around [::update/update ::after-update]
  [query-type model {::keys [doing-after-update?], :as parsed-args}]
  (if doing-after-update?
    (next-method query-type model parsed-args)
    (u/with-debug-result ["do after-update for %s in %s" model `update/reducible-pks*]
      (let [parsed-args                    (assoc parsed-args ::doing-after-update? true)
            reducible-pks (next-method query-type model parsed-args)]
        (eduction
         (map (select/select-pks-fn model))
         (->ReducibleAfterUpdate model reducible-pks))))))

(defmacro define-after-update
  {:style/indent :defn}
  [model [result-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-update)
     (m/defmethod after-update model#
       [~'&model ~result-binding]
       ~@body)))
