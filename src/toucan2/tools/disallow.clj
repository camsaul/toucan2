(ns toucan2.tools.disallow
  (:require
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]))

(m/defmethod pipeline/transduce-with-model [:toucan.query-type/select.* ::select]
  [_rf _query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot select %s." model))))

(m/defmethod pipeline/transduce-with-model [:toucan.query-type/delete.* ::delete]
  [_rf _query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot delete instances of %s." model))))

(m/defmethod pipeline/transduce-with-model [:toucan.query-type/insert.* ::insert]
  [_rf _query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot create new instances of %s." model))))

(m/defmethod pipeline/transduce-with-model [:toucan.query-type/update.* ::update]
  [_rf _query-type model _parsed-args]
  (throw (UnsupportedOperationException. (format "You cannot update a %s after it has been created." model))))
