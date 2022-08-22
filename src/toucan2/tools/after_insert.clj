(ns toucan2.tools.after-insert
  (:require
   [methodical.core :as m]
   [toucan2.insert :as insert]
   [toucan2.tools.after :as tools.after]
   [toucan2.util :as u]))

(derive ::insert/insert ::tools.after/after)
(derive ::after-insert ::tools.after/after)

(defmacro define-after-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-insert)
     (m/defmethod tools.after/after [::insert/insert model#]
       [~'&query-type ~'&model ~instance-binding]
       ~@body)))
