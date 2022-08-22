(ns toucan2.tools.after-select
  (:require
   [methodical.core :as m]
   [toucan2.select :as select]
   [toucan2.tools.after :as tools.after]
   [toucan2.util :as u]))

(derive ::select/select ::tools.after/after)
(derive ::after-select ::tools.after/after)

(defmacro define-after-select
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-select)
     (m/defmethod tools.after/after [::select/select model#]
       [~'&query-type ~'&model ~instance-binding]
       ~@body)))
