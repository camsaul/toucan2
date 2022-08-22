(ns toucan2.tools.after-update
  (:require
   [methodical.core :as m]
   [toucan2.tools.after :as tools.after]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(derive ::update/update ::tools.after/after)
(derive ::after-update ::tools.after/after)

(defmacro define-after-update
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-update)
     (m/defmethod tools.after/after [::update/update model#]
       [~'&query-type ~'&model ~instance-binding]
       ~@body)))
