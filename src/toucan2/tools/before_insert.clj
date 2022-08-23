(ns toucan2.tools.before-insert
  (:require
   [methodical.core :as m]
   [toucan2.insert :as insert]
   [toucan2.operation :as op]
   [toucan2.util :as u]))

(m/defmulti before-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(defn do-before-insert-to-rows [rows model]
  (mapv
   (fn [row]
     (try
       (u/with-debug-result ["Do before-insert to %s" row]
         (before-insert model row))
       (catch Throwable e
         (throw (ex-info (format "Error in before-insert for %s: %s" (u/safe-pr-str model) (ex-message e))
                         {:context u/*error-context*, :model model, :row row}
                         e)))))
   rows))

(m/defmethod op/reducible* :before [::insert/insert ::before-insert]
  [_query-type model parsed-args]
  (assert (map? parsed-args))
  (u/with-debug-result ["Do before insert for %s" model]
    (update parsed-args :rows do-before-insert-to-rows model)))

;;; Important! before-insert should be done BEFORE any [[toucan2.tools.transformed/transforms]]. Transforms are often
;;; for serializing and deserializing values; we don't want before insert methods to have to work with
;;; already-serialized values.
;;;
;;; By marking `::before-insert` as preferred over `:toucan2.tools.transformed/transformed` it will be done first (see
;;; https://github.com/camsaul/methodical#before-methods)
(m/prefer-method! #'op/reducible*
                  [::insert/insert ::before-insert]
                  [::insert/insert :toucan2.tools.transformed/transformed])

(defmacro define-before-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-insert)
     (m/defmethod before-insert model#
       [~'&model ~instance-binding]
       (cond->> (do ~@body)
         ~'next-method (~'next-method ~'&model)))))
