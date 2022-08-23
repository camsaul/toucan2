(ns toucan2.tools.with-temp
  (:require
   [clojure.test :as t]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.model :as model]
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

  ```clj
  (m/defmethod do-with-temp* :before :default
    [model attributes f]
    (set-up-db!)
    f)
  ```

  `explicit-attributes` are the attributes specified in the `with-temp` form itself, any may be `nil`. The default
  implementation merges the attributes from [[with-temp-defaults]] like

  ```clj
  (merge {} (with-temp-defaults model) explict-attributes)
  ```"
  {:arglists '([model explicit-attributes f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-temp* :default
  [model explicit-attributes f]
  (assert (some? model) (format "%s model cannot be nil." `with-temp))
  (let [defaults          (with-temp-defaults model)
        merged-attributes (merge {} defaults explicit-attributes)]
    (binding [u/*error-context* (update u/*error-context*
                                        ::with-temp
                                        (fn [with-temp]
                                          (conj with-temp {:model               model
                                                           :explicit-attributes explicit-attributes
                                                           :default-attributes  defaults
                                                           :merged-attributes   merged-attributes})))]
      (let [temp-object (u/with-debug-result ["Create temporary %s with attributes %s" model merged-attributes]
                          (first (try
                                   (insert/insert-returning-instances! model merged-attributes)
                                   (catch Throwable e
                                     (throw (ex-info (format "Error inserting temp %s: %s"
                                                             (u/safe-pr-str model) (ex-message e))
                                                     ;; just take the stuff we added to the error context above instead
                                                     ;; of defining it all a second time.
                                                     (merge {:context u/*error-context*}
                                                            (last (::with-temp u/*error-context*)))
                                                     e))))))]

        (try
          (t/testing (format "with temporary %s with attributes %s" (u/safe-pr-str model) (u/safe-pr-str merged-attributes))
            (f temp-object))
          (finally
            (delete/delete! model :toucan/pk ((model/select-pks-fn model) temp-object))))))))

(defn do-with-temp [modelable attributes f]
  (model/with-model [model modelable]
    (do-with-temp* model attributes f)))

(defmacro with-temp
  "Define a temporary instance of a model and bind it to `temp-object-binding`. The object is inserted
  using [[insert/insert-returning-instances!]] using the values from [[with-temp-defaults]] merged with `attributes`. At
  the conclusion of `body`, the object is deleted. This is primarily intended for usage in tests, so this adds
  a [[clojure.test/testing]] context around `body` as well.

  [[with-temp]] can create multiple objects in one form if you pass additional bindings.

  `temp-object-binding` and `attributes` are optional, and default to `_` and `nil`, respectively. If you're creating
  multiple objects at once these must be explicitly specified.

  Examples:

  ```clj
  ;;; use the with-temp-defaults for :models/bird
  (with-temp [:models/bird bird]
    (do-something bird))

  ;;; use the with-temp-defaults for :models/bird merged with {:name \"Lucky Pigeon\"}
  (with-temp [:models/bird bird {:name \"Lucky Pigeon\"}]
    (do-something bird))

  ;;; define multiple instances at the same time
  (with-temp [:models/bird bird-1 {:name \"Parroty\"}
              :models/bird bird-2 {:name \"Green Friend\", :best-friend-id (:id bird-1)}]
    (do-something bird))
  ```

  If you want to implement custom behavior for a model other than default values, you can implement [[do-with-temp*]]."
  [[modelable temp-object-binding attributes & more] & body]
  `(do-with-temp ~modelable ~attributes
                 (^:once fn* [~(or temp-object-binding '_)]
                  ~(if (seq more)
                     `(with-temp ~(vec more) ~@body)
                     `(do ~@body)))))
