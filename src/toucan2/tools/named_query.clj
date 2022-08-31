(ns toucan2.tools.named-query
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]))

;;;; This doesn't NEED to be a macro but having the definition live in the namespace it was defined in is useful for
;;;; stack trace purposes. Also it lets us do validation with the spec below.
(defmacro define-named-query
  "Helper for defining 'named' queries.

  ```clj
  ;; define a custom query ::my-count that you can then use with select and the like
  (define-named-query ::my-count
    {:select [:%count.*], :from [(keyword (model/table-name model))]})

  (select :model/user ::my-count)
  ```"
  {:style/indent 1}
  ([query-name resolved-query]
   `(define-named-query ~query-name :default :default ~resolved-query))

  ([query-name query-type model resolved-query]
   `(m/defmethod pipeline/transduce-resolve [~query-type ~model ~query-name]
      [rf# ~'&query-type ~'&model ~'&parsed-args ~'&unresolved-query]
      (~'next-method rf# ~'&query-type ~'&model ~'&parsed-args ~resolved-query))))

(s/fdef define-named-query
  :args (s/alt :2-arity (s/cat :query-name     (every-pred keyword? namespace)
                               :resolved-query some?)
               :4-arity (s/cat :query-name     (every-pred keyword? namespace)
                               :query-type     (s/alt :query-type pipeline/query-type?
                                                      :default    #{:default})
                               :model          some?
                               :resolved-query some?))
  :ret any?)
