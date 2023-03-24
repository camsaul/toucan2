(ns toucan.test-models.user
  "A very simple model for testing out basic DB functionality."
  (:require
   [toucan.models :as t1.models]))

;;; Work around https://github.com/clj-kondo/clj-kondo/issues/2026
#_{:clj-kondo/ignore [:invalid-arity]}
(t1.models/defmodel User :t1_users)
