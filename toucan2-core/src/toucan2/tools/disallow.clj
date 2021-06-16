(ns toucan2.tools.disallow
  (:require [methodical.core :as m]
            [toucan2.util :as u]
            [toucan2.select :as select]
            [toucan2.mutative :as mutative]))

(m/defmethod select/select* [:default :toucan2/disallow-select :default]
  [_ tableable _ _]
  (throw (UnsupportedOperationException. (format "You cannot select %s." (u/dispatch-value tableable)))))

(m/defmethod mutative/delete!* [:default :toucan2/disallow-delete :default]
  [_ tableable _ _]
  (throw (UnsupportedOperationException. (format "You cannot delete instances of %s." (u/dispatch-value tableable)))))

(m/defmethod mutative/insert!* [:default :toucan2/disallow-insert :default]
  [_ tableable _ _]
  (throw (UnsupportedOperationException. (format "You cannot create new instances of %s." (u/dispatch-value tableable)))))

(m/defmethod mutative/update!* [:default :toucan2/disallow-update :default]
  [_ tableable _ _]
  (throw (UnsupportedOperationException. (format "You cannot update a %s after it has been created." (u/dispatch-value tableable)))))
