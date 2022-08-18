(ns toucan2.tools.with-temp
  (:require
   [clojure.test :as t]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.model :as model]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(m/defmulti with-temp-defaults
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod with-temp-defaults :default
  [_model]
  nil)

(defn do-with-temp [modelable attributes f]
  (model/with-model [model modelable]
    (let [defaults          (with-temp-defaults model)
          merged-attributes (merge {} defaults attributes)
          [pk temp-object]  (u/with-debug-result ["Create temporary %s with attributes %s" model merged-attributes]
                              (let [[pk] (try
                                           (insert/insert-returning-pks! model merged-attributes)
                                           (catch Throwable e
                                             (throw (ex-info (format "Error inserting temp %s: %s" (pr-str model) (ex-message e))
                                                             {:model  model
                                                              :attributes {:parameters attributes
                                                                           :default    defaults
                                                                           :merged     merged-attributes}}
                                                             e))))]
                                [pk (select/select-one model pk)]))]

      (try
        (t/testing (format "with temporary %s with attributes %s" (pr-str model) (pr-str merged-attributes))
          (f temp-object))
        (finally
          (delete/delete! model pk))))))

(defmacro with-temp [[modelable temp-object-binding attributes & more] & body]
  `(do-with-temp ~modelable ~attributes
                 (fn [~temp-object-binding]
                   ~(if (seq more)
                      `(with-temp ~(vec more) ~@body)
                      `(do ~@body)))))
