(ns toucan.test-models.address
  (:require
   [toucan.models :as t1.models]))

;;; Work around https://github.com/clj-kondo/clj-kondo/issues/2026
#_{:clj-kondo/ignore [:invalid-arity]}
(t1.models/defmodel Address :t1_address)
