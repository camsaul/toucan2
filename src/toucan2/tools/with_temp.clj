(ns toucan2.tools.with-temp
  (:require
   [clojure.pprint :as pprint]
   [clojure.spec.alpha :as s]
   [clojure.test :as t]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.util :as u]))

(swap! log/all-topics conj :with-temp)

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
    [_model _explicit-attributes f]
    (set-up-db!)
    f)
  ```

  `explicit-attributes` are the attributes specified in the `with-temp` form itself, any may be `nil`. The default
  implementation merges the attributes from [[with-temp-defaults]] like

  ```clj
  (merge {} (with-temp-defaults model) explict-attributes)
  ```"
  {:arglists '([model₁ explicit-attributes f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-temp* :default
  [model explicit-attributes f]
  (assert (some? model) (format "%s model cannot be nil." `with-temp))
  (when (some? explicit-attributes)
    (assert (map? explicit-attributes) (format "attributes passed to %s must be a map." `with-temp)))
  (let [defaults          (with-temp-defaults model)
        merged-attributes (merge {} defaults explicit-attributes)]
    (u/try-with-error-context ["with temp" {::model               model
                                            ::explicit-attributes explicit-attributes
                                            ::default-attributes  defaults
                                            ::merged-attributes   merged-attributes}]
      (log/debugf :with-temp "Create temporary %s with attributes %s" model merged-attributes)
      (let [temp-object (first (insert/insert-returning-instances! model merged-attributes))]
        (log/debugf :with-temp "[with-temp] => %s" temp-object)
        (try
          (t/testing (format "\nwith temporary %s with attributes\n%s\n"
                             (pr-str model)
                             (with-out-str (pprint/pprint merged-attributes)))
            (f temp-object))
          (finally
            (delete/delete! model :toucan/pk ((model/select-pks-fn model) temp-object))))))))

(defn do-with-temp [modelable attributes f]
  (let [model (model/resolve-model modelable)]
    (do-with-temp* model attributes f)))

(defmacro with-temp
  "Define a temporary instance of a model and bind it to `temp-object-binding`. The object is inserted
  using [[insert/insert-returning-instances!]] using the values from [[with-temp-defaults]] merged with a map of
  `attributes`. At the conclusion of `body`, the object is deleted. This is primarily intended for usage in tests, so
  this adds a [[clojure.test/testing]] context around `body` as well.

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
                 (^:once fn* [temp-object#]
                  (let [~(or temp-object-binding '_) temp-object#]
                    ~(if (seq more)
                       `(with-temp ~(vec more) ~@body)
                       `(do ~@body))))))

(s/fdef with-temp
  :args (s/cat :bindings (s/spec (s/cat
                                  :model+binding+attributes
                                  (s/* (s/cat :model      some?
                                              :binding    :clojure.core.specs.alpha/binding-form
                                              :attributes any?))

                                  :model+optional
                                  (s/cat :model    some?
                                         :optional (s/? (s/cat :binding    :clojure.core.specs.alpha/binding-form
                                                               :attributes (s/? any?))))))
               :body     (s/* any?))
  :ret  any?)
