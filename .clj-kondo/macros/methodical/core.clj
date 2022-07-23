(ns macros.methodical.core
  (:refer-clojure :exclude [defmethod]))

(defn add-next-method [fn-tail]
  (if (vector? (first fn-tail))
    (cons (into ['next-method] (first fn-tail))
          (rest fn-tail))
    (map add-next-method fn-tail)))

(defmacro defmethod
  [multimethod & args]
  (let [[aux-qualifier dispatch-value & fn-tail] (if (#{:before :after :around} (first args))
                                                   args
                                                   (cons nil args))
        fn-tail                                  (if (#{:around nil} aux-qualifier)
                                                   (add-next-method fn-tail)
                                                   fn-tail)]
    `(clojure.core/defmethod ~multimethod ~dispatch-value
       ~@fn-tail)))
