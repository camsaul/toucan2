(ns toucan2.tools.after-insert
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.insert :as insert]
   [toucan2.operation :as op]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(m/defmulti after-insert
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod after-insert :around :default
  [model instance]
  (u/with-debug-result [(list `after-insert model instance)]
    (try
      (next-method model instance)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `after-insert (pr-str model) (ex-message e))
                        {:model model, :row instance}
                        e))))))

(defn after-reducible-instances [model reducible-instances]
  (eduction
   (map (fn [row]
          (after-insert model row)))
   reducible-instances))

(defrecord AfterReduciblePKs [model reducible-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (u/with-debug-result ["reducing after-insert for %s" model]
      (try
        (let [affected-pks (realize/realize reducible-pks)]
          (u/println-debug ["Doing after-insert for %s with PKs %s" model affected-pks])
          (reduce
           rf
           init
           (after-reducible-instances
            model
            (select/select-reducible-with-pks model affected-pks))))
        (catch Throwable e
          (throw (ex-info (format "Error doing after insert for %s: %s" (pr-str model) (ex-message e))
                          {:model model, :parent-insert reducible-pks}
                          e))))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->AfterReduciblePKs model reducible-pks)))

(m/defmethod op/reducible* :around [::insert/insert ::after-insert]
  [query-type model {::keys [after-insert?], :as parsed-args}]
  (if after-insert?
    (next-method query-type model parsed-args)
    (u/with-debug-result ["do after-insert for %s in %s" model `insert/reducible-insert*]
      (let [parsed-args     (assoc parsed-args ::after-insert? true)
            reducible-count (next-method query-type model parsed-args)
            reducible-pks   (select/return-pks-eduction model reducible-count)]
        (eduction
         (map (constantly 1))
         (->AfterReduciblePKs model reducible-pks))))))

(m/defmethod op/reducible-returning-pks* :around [::insert/insert ::after-insert]
  [query-type model {::keys [after-insert?], :as parsed-args}]
  (if after-insert?
    (next-method query-type model parsed-args)
    (u/with-debug-result ["do after-insert for %s in %s" model `insert/reducible-insert-returning-pks*]
      (let [parsed-args   (assoc parsed-args ::after-insert? true)
            reducible-pks (next-method query-type model parsed-args)]
        (eduction
         (map (select/select-pks-fn model))
         (->AfterReduciblePKs model reducible-pks))))))

(m/defmethod op/reducible-returning-instances* :around [::insert/insert ::after-insert]
  [query-type model {::keys [after-insert?], :as parsed-args}]
  (if after-insert?
    (next-method query-type model parsed-args)
    (let [parsed-args (assoc parsed-args ::after-insert? true)]
      (after-reducible-instances model (next-method query-type model parsed-args)))))

(defmacro define-after-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-insert)
     (m/defmethod after-insert model#
       [~'&model ~instance-binding]
       ~@body)))
