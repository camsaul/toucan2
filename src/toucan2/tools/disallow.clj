(ns toucan2.tools.disallow
  (:require
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.operation :as op]
   [toucan2.select :as select]
   [toucan2.update :as update]))

(m/defmethod select/select-reducible* ::select
  [model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot select %s." model))))

(m/defmethod op/reducible* [::delete/delete ::delete]
  [model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot delete instances of %s." model))))

(m/defmethod op/reducible* [::insert/insert ::insert]
  [model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot create new instances of %s." model))))

(m/defmethod op/reducible* [::update/update ::update]
  [model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot update a %s after it has been created." model))))
