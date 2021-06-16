(ns toucan2.tools.with-temp
  (:require [clojure.test :refer [testing]]
            [methodical.core :as m]
            [toucan2.log :as log]
            [toucan2.mutative :as mutative]
            [toucan2.select :as select]
            [toucan2.util :as u]))

(m/defmulti with-temp-defaults*
  {:arglists '([tableableᵈᵗ])}
  u/dispatch-on-first-arg)

(m/defmethod with-temp-defaults* :default
  [_]
  nil)

(defn do-with-temp [tableable attributes f]
  (let [defaults          (with-temp-defaults* tableable)
        merged-attributes (merge {} defaults attributes)
        [pk temp-object]  (log/with-trace ["Create temporary %s with attributes %s" tableable merged-attributes]
                            (let [[pk] (try
                                         (mutative/insert-returning-keys! tableable merged-attributes)
                                         (catch Throwable e
                                           (throw (ex-info (format "Error inserting temp %s: %s" (pr-str tableable) (ex-message e))
                                                           {:tableable  tableable
                                                            :attributes {:parameters attributes
                                                                         :default    defaults
                                                                         :merged     merged-attributes}}
                                                           e))))]
                              [pk (select/select-one tableable pk)]))]

    (try
      (testing (format "with temporary %s with attributes %s" (pr-str tableable) (pr-str merged-attributes))
        (f temp-object))
      (finally
        (mutative/delete! tableable pk)))))

(defmacro with-temp [[tableable temp-object-binding attributes & more] & body]
  `(do-with-temp ~tableable ~attributes
                 (fn [~temp-object-binding]
                   ~(if (seq more)
                      `(with-temp ~(vec more) ~@body)
                      `(do ~@body)))))
