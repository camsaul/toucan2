(ns toucan2.types
  "Toucan 2 query type hierarchy."
  (:require [clojure.spec.alpha :as s]))

;;; the query type hierarchy below is used for pipeline methods and tooling to decide what sort of things they need to
;;; do -- for example you should not do row-map transformations to a query that returns an update count.

(derive :toucan.query-type/select.* :toucan.query-type/*)
(derive :toucan.query-type/insert.* :toucan.query-type/*)
(derive :toucan.query-type/update.* :toucan.query-type/*)
(derive :toucan.query-type/delete.* :toucan.query-type/*)

;;; `DML` (Data manipulation language) here means things like `UPDATE`, `DELETE`, or `INSERT`. Some goofballs include
;;; `SELECT` in this category, but we are not! We are calling `SELECT` a `DQL` (Data Query Language) statement. There
;;; are other types of queries like `DDL` (Data Definition Language, e.g. `CREATE TABLE`), but Toucan 2 doesn't
;;; currently have any tooling around those. Stuff like [[toucan2.execute/query]] that could potentially execute those
;;; don't care what kind of query you're executing anyway.

(derive :toucan.statement-type/DML :toucan.statement-type/*)
(derive :toucan.statement-type/DQL :toucan.statement-type/*)

(derive :toucan.query-type/select.* :toucan.statement-type/DQL)
(derive :toucan.query-type/insert.* :toucan.statement-type/DML)
(derive :toucan.query-type/update.* :toucan.statement-type/DML)
(derive :toucan.query-type/delete.* :toucan.statement-type/DML)

(derive :toucan.result-type/instances    :toucan.result-type/*)
(derive :toucan.result-type/pks          :toucan.result-type/*)
(derive :toucan.result-type/update-count :toucan.result-type/*)

(doto :toucan.query-type/select.instances
  (derive :toucan.query-type/select.*)
  (derive :toucan.result-type/instances))

;;; [[toucan2.select/select-fn-set]] and [[toucan2.select/select-fn-vec]] queries -- we are applying a specific function
;;; transform to the results, so we don't want to apply a default fields transform or other stuff like that.
(derive :toucan.query-type/select.instances.fns :toucan.query-type/select.instances)

;;; A special subtype of a SELECT query that should use the syntax of update. Used to
;;; power [[toucan2.tools.before-update]].
;;;
;;; The difference is that update is supposed to treat a resolved query map as a conditions map rather than a Honey SQL
;;; form.
(derive :toucan.query-type/select.instances.from-update :toucan.query-type/select.instances)

(doto :toucan.query-type/insert.update-count
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/insert.pks
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/insert.instances
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/instances))

(doto :toucan.query-type/update.update-count
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/update.pks
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/update.instances
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/instances))

(doto :toucan.query-type/delete.update-count
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/delete.pks
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/delete.instances
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/instances))

(defn query-type?
  "True if `query-type` derives from one of the various abstract query keywords such as `:toucan.result-type/*` or
  `:toucan.query-type/*`. This does not guarantee that the query type is a 'concrete', just that it is something with
  some sort of query type information."
  [query-type]
  (some (fn [abstract-type]
          (isa? query-type abstract-type))
        [:toucan.result-type/*
         :toucan.query-type/*
         :toucan.statement-type/*]))

;;;; utils

(defn parent-query-type [query-type]
  (some (fn [k]
          (when (isa? k :toucan.query-type/*)
            k))
        (parents query-type)))

(defn base-query-type
  "E.g. something like `:toucan.query-type/insert.*`. The immediate descendant of `:toucan.query-type/*`.

  ```clj
  (base-query-type :toucan.query-type/insert.instances)
  =>
  :toucan.query-type/insert.*
  ```"
  [query-type]
  (when (isa? query-type :toucan.query-type/*)
    (loop [last-query-type nil, query-type query-type]
      (if (or (= query-type :toucan.query-type/*)
              (not query-type))
        last-query-type
        (recur query-type (parent-query-type query-type))))))

(defn similar-query-type-returning
  "```clj
  (similar-query-type-returning :toucan.query-type/insert.instances :toucan.result-type/pks)
  =>
  :toucan.query-type/insert.pks
  ```"
  [query-type result-type]
  (let [base-type (base-query-type query-type)]
    (some (fn [descendant]
            (when (and ((parents descendant) base-type)
                       (isa? descendant result-type))
              descendant))
          (descendants base-type))))

(s/def ::dispatch-value.default
  (partial = :default))

;;; `:toucan.query-type/abstract` exists for things that aren't actually supposed to go in the query type hierarchy, but
;;; we want to be able to derive other query types FROM them. See [[toucan2.tools.after]] and
;;; `:toucan2.tools.after/query-type` for example.
(s/def ::dispatch-value.query-type
  (s/or :default             ::dispatch-value.default
        :abstract-query-type #(isa? % :toucan.query-type/abstract)
        :query-type          query-type?))

;; technically `nil` is valid and it's not read in as a Symbol
(s/def ::dispatch-value.keyword-or-class
  (some-fn keyword? symbol? nil?))

(s/def ::dispatch-value.model
  ::dispatch-value.keyword-or-class)

(s/def ::dispatch-value.resolved-query
  ::dispatch-value.keyword-or-class)

(s/def ::dispatch-value.query-type-model
  (s/or :default          ::dispatch-value.default
        :query-type-model (s/cat :query-type ::dispatch-value.query-type
                                 :model      ::dispatch-value.model)))

(s/def ::dispatch-value.query-type-model-resolved-query
  (s/or
   :default                         ::dispatch-value.default
   :query-type-model-resolved-query (s/cat :query-type     ::dispatch-value.query-type
                                           :model          ::dispatch-value.model
                                           :resolved-query ::dispatch-value.resolved-query)))
