(ns toucan2.util
  (:require
   [clojure.spec.alpha :as s]
   [clojure.walk :as walk]
   [pretty.core :as pretty]
   [toucan2.protocols :as protocols]))

;;; TODO -- there is a lot of repeated code in here to make sure we don't accidentally realize and print `IReduceInit`,
;;; and at least 3 places we turn an `eduction` into the same pretty form. Maybe we should try to consolidate some of
;;; that logic.

(set! *warn-on-reflection* true)

(defn dispatch-on-first-arg
  "Dispatch on the first argument using [[dispatch-value]], and ignore all other args."
  [x & _]
  (protocols/dispatch-value x))

(defn dispatch-on-first-two-args
  "Dispatch on the two arguments using [[protocols/dispatch-value]], and ignore all other args."
  [x y & _]
  [(protocols/dispatch-value x) (protocols/dispatch-value y)])

(defn dispatch-on-first-three-args
  "Dispatch on the three arguments using [[protocols/dispatch-value]], and ignore all other args."
  [x y z & _]
  [(protocols/dispatch-value x) (protocols/dispatch-value y) (protocols/dispatch-value z)])

(defn lower-case-en
  "Locale-agnostic version of [[clojure.string/lower-case]]. `clojure.string/lower-case` uses the default locale in
  conversions, turning `ID` into `ıd`, in the Turkish locale. This function always uses the `Locale/US` locale."
  [^CharSequence s]
  (.. s toString (toLowerCase (java.util.Locale/US))))

(defn maybe-derive
  "Derive `child` from `parent` only if `child` is not already a descendant of `parent`."
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))

;;;; [[try-with-error-context]]

(defprotocol ^:private AddContext
  (^:no-doc add-context ^Throwable [^Throwable e additional-context]))

(defn- add-context-to-ex-data [ex-data-map additional-context]
  (update ex-data-map
          :toucan2/context-trace
          #(conj (vec %) (walk/prewalk
                          (fn [form]
                            (cond
                              (instance? pretty.core.PrettyPrintable form)
                              (pretty/pretty form)

                              (instance? clojure.core.Eduction form)
                              (list 'eduction
                                    (.xform ^clojure.core.Eduction form)
                                    (.coll ^clojure.core.Eduction form))

                              (and (instance? clojure.lang.IReduceInit form)
                                   (not (coll? form)))
                              (class form)

                              :else
                              form))
                          additional-context))))

(extend-protocol AddContext
  clojure.lang.ExceptionInfo
  (add-context [^Throwable e additional-context]
    (if (empty? additional-context)
      e
      (doto ^Throwable (ex-info (ex-message e)
                                (add-context-to-ex-data (ex-data e) additional-context)
                                (ex-cause e))
        (.setStackTrace (.getStackTrace e)))))

  Throwable
  (add-context [^Throwable e additional-context]
    (if (empty? additional-context)
      e
      (doto ^Throwable (ex-info (ex-message e)
                                (add-context-to-ex-data {} additional-context)
                                e)
        (.setStackTrace (.getStackTrace e))))))

(defmacro try-with-error-context
  {:style/indent :defn}
  [additional-context & body]
  `(try
     ~@body
     (catch Exception e#
       (throw (add-context e# ~additional-context)))
     (catch AssertionError e#
       (throw (add-context e# ~additional-context)))))

(s/fdef try-with-error-context
  :args (s/cat :additional-context (s/alt :message+map (s/spec (s/cat :message string?
                                                                      :map     map?))
                                          ;; some sort of function call or something like that.
                                          :form        seqable?)
               :body               (s/+ any?))
  :ret  any?)
