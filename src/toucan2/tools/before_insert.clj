(ns toucan2.tools.before-insert
  (:require
   [methodical.core :as m]
   [toucan2.insert :as insert]
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
         (throw (ex-info (format "Error in before-insert for %s: %s" (pr-str model) (ex-message e))
                         {:model model, :row row}
                         e)))))
   rows))

(m/defmethod insert/reducible-insert* :before ::before-insert
  [model parsed-args]
  (u/with-debug-result ["Do before insert for %s" model]
    (update parsed-args :rows do-before-insert-to-rows model)))

(defmacro define-before-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-insert)
     (m/defmethod before-insert model#
       [~'&model ~instance-binding]
       (cond->> (do ~@body)
         ~'next-method (~'next-method ~'&model)))))
