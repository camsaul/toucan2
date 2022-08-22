(ns toucan2.tools.after
  "Shared low-level implementation for doing transformations to the results of different operations defined
  with [[toucan2.operation]]."
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

;;; TODO -- Consider whether the various methods here should be `:after` methods rather than `:around`

(m/defmulti after
  {:arglists '([query-type model instance])}
  u/dispatch-on-first-two-args)

(m/defmethod after :around :default
  [query-type model instance]
  (u/with-debug-result [(list `after query-type model instance)]
    (try
      (next-method query-type model instance)
      (catch Throwable e
        (throw (ex-info (format "Error in %s %s for %s: %s" `after query-type (u/safe-pr-str model) (ex-message e))
                        {:model model, :row instance}
                        e))))))

(defn has-after-method? [query-type model]
  (not= (m/default-primary-method after)
        (m/effective-primary-method after (m/dispatch-value after query-type model))))

;;;; reducible instances

(defn after-reducible-instances [query-type model reducible-instances]
  (eduction
   (map (fn [row]
          (or (some-> (after query-type model row) instance/reset-original)
              row)))
   reducible-instances))

(m/defmethod op/reducible-returning-instances* :around [::after ::after]
  [query-type model parsed-args]
  (cond
    (::doing-after? parsed-args)
    (next-method query-type model parsed-args)

    (not (has-after-method? query-type model))
    (next-method query-type model parsed-args)

    :else
    (let [parsed-args (assoc parsed-args ::doing-after? true)]
      (after-reducible-instances query-type model (next-method query-type model parsed-args)))))

;;;; reducible PKs

(deftype AfterReduciblePKs [query-type model reducible-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (u/with-debug-result ["reducing %s %s for %s" `after query-type model]
      (let [affected-pks (realize/realize reducible-pks)]
        (u/println-debug ["Doing %s %s for %s with PKs %s" `after query-type model affected-pks])
        (reduce
         rf
         init
         (after-reducible-instances
          query-type
          model
          (op/select-reducible-with-pks model nil affected-pks))))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->AfterReduciblePKs query-type model reducible-pks)))

(m/defmethod op/reducible-returning-pks* :around [::after ::after]
  [query-type model parsed-args]
  (cond
    (::doing-after? parsed-args)
    (next-method query-type model parsed-args)

    (not (has-after-method? query-type model))
    (next-method query-type model parsed-args)

    :else
    (let [parsed-args   (assoc parsed-args ::doing-after? true)
          reducible-pks (next-method query-type model parsed-args)]
      (assert (instance? clojure.lang.IReduceInit reducible-pks))
      (eduction
       (map (model/select-pks-fn model))
       (->AfterReduciblePKs query-type model reducible-pks)))))

;;;; reducible update count

(m/defmethod op/reducible* :around [::after ::after]
  [query-type model parsed-args]
  (cond
    (::doing-after? parsed-args)
    (next-method query-type model parsed-args)

    (not (has-after-method? query-type model))
    (next-method query-type model parsed-args)

    :else
    (let [parsed-args     (assoc parsed-args ::doing-after? true)
          reducible-count (next-method query-type model parsed-args)
          reducible-pks   (op/return-pks-eduction model reducible-count)]
      (eduction
       (map (constantly 1))
       (->AfterReduciblePKs query-type model reducible-pks)))))
