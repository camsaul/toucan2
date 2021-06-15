(ns toucan2.experimental
  (:require [methodical.core :as m]
            [toucan2.mutative :as mutative]
            [toucan2.select :as select]))

;; EXPERIMENTAL

(m/defmethod mutative/insert!* :around [:default ::insert-return-instances :default]
  [connectable tableable query options]
  (next-method connectable tableable query (assoc-in options [:next.jdbc :return-keys] true)))

(m/defmethod mutative/insert!* :after [:default ::insert-return-instances :default]
  [connectable tableable rows options]
  (let [select-pks-fn (select/select-pks-fn connectable tableable)
        ;; we will probably get back instances since this is a result of `insert!`... but in case this is the result
        ;; of `insert-returning-keys!` we'd get back plain pks
        ;;
        ;; TODO -- is this even possible?
        pks           (for [row rows]
                        (if (map? row)
                          (select-pks-fn row)
                          row))]
    (select/select [connectable tableable] :toucan2/with-pks pks {} (update options :next.jdbc dissoc :return-keys))))
