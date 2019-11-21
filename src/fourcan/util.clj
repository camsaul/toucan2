(ns fourcan.util)

(defn mapply
  ([f]
   (f))

  ([f & args]
   (apply f (concat (butlast args) (reduce concat (last args))))))
