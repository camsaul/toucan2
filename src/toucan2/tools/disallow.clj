(ns toucan2.tools.disallow
  (:require
   [methodical.core :as m]
   [toucan2.operation :as op]))

(m/defmethod op/reducible-returning-instances* [:toucan2.select/select ::select]
  [_query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot select %s." model))))

(m/defmethod op/reducible-update* [:toucan2.delete/delete ::delete]
  [_query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot delete instances of %s." model))))

(m/defmethod op/reducible-update* [:toucan2.insert/insert ::insert]
  [_query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot create new instances of %s." model))))

(m/defmethod op/reducible-update* [:toucan2.update/update ::update]
  [_query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot update a %s after it has been created." model))))
