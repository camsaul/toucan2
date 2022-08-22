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

(m/defmulti do-with-temp*
  "Implementation of [[with-temp]]. You can implement this if you need to do some sort of special behavior for a
  particular model. But normally you would just implement [[with-temp-defaults]]. If you need to do special setup when
  using [[with-temp]], you can implement a `:before` method:

    (m/defmethod do-with-temp* :before :default
      [model attributes f]
      (set-up-db!)
      f)"
  {:arglists '([model attributes f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-temp* :default
  [model attributes f]
  (let [defaults          (with-temp-defaults model)
        merged-attributes (merge {} defaults attributes)
        [pk temp-object]  (u/with-debug-result ["Create temporary %s with attributes %s" model merged-attributes]
                            (let [[pk] (try
                                         (insert/insert-returning-pks! model merged-attributes)
                                         (catch Throwable e
                                           (throw (ex-info (format "Error inserting temp %s: %s" (u/safe-pr-str model) (ex-message e))
                                                           {:model  model
                                                            :attributes {:parameters attributes
                                                                         :default    defaults
                                                                         :merged     merged-attributes}}
                                                           e))))]
                              [pk (select/select-one model :toucan/pk pk)]))]

    (try
      (t/testing (format "with temporary %s with attributes %s" (u/safe-pr-str model) (u/safe-pr-str merged-attributes))
        (f temp-object))
      (finally
        (delete/delete! model :toucan/pk pk)))))

(defn do-with-temp [modelable attributes f]
  (model/with-model [model modelable]
    (do-with-temp* model attributes f)))

(defmacro with-temp [[modelable temp-object-binding attributes & more] & body]
  `(do-with-temp ~modelable ~attributes
                 (fn [~temp-object-binding]
                   ~(if (seq more)
                      `(with-temp ~(vec more) ~@body)
                      `(do ~@body)))))
