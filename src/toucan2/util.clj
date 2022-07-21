(ns toucan2.util)

(defn keyword-or-type [x]
  (if (keyword? x)
    x
    (type x)))

(defn dispatch-on-keyword-or-type-1
  [x & _]
  (keyword-or-type x))

(defn dispatch-on-keyword-or-type-2
  [x y & _]
  [(keyword-or-type x)
   (keyword-or-type y)])
