(ns bluejdbc.query
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.statement :as statement]
            [pretty.core :as pretty]))

(defn reducible-query
  ([conn query]
   (reducible-query conn query nil))

  ([conn query options]
   (reify
     pretty/PrettyPrintable
     (pretty [_]
       (list 'reducible-query query))

     clojure.lang.IReduceInit
     (reduce [_ rf init]
       (with-open [conn (conn/connection conn options)
                   stmt (statement/prepared-statement conn query options)
                   rs   (.executeQuery stmt)]
         (reduce rf init (result-set/reducible-results rs options))))

     clojure.lang.IReduce
     (reduce [this rf]
       (reduce rf [] this)))))

(defn query
  ([conn a-query]
   (query conn a-query nil))

  ([conn query options]
   (reduce conj [] (reducible-query conn query options))))

(defn query-one
  ([conn query]
   (query-one conn query nil))

  ([conn a-query options]
   (first (query conn a-query (update options :results/xform (fn [xform]
                                                               (let [xform (or xform result-set/maps-xform)]
                                                                 (fn [rs]
                                                                   (comp (take 1) (xform rs))))))))))
