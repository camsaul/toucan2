(ns toucan2.current)

;; TODO -- `:toucan/default` or just `:default`?
;;
;; TODO -- Maybe this should actually just be `nil` and we'll write a `do-with-connection` method for `nil`
;;
;; TODO -- move this to [[toucan2.connection]] and rename it to `*current-connectable*`
(def ^:dynamic *connectable* :toucan/default)
