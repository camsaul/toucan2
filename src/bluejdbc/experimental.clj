(ns bluejdbc.experimental
  (:require [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [methodical.core :as m]))

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
    (select/select [connectable tableable] :bluejdbc/with-pks pks {} (update options :next.jdbc dissoc :return-keys))))
