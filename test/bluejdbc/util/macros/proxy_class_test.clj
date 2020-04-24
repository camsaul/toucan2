(ns bluejdbc.util.macros.proxy-class-test
  (:require [bluejdbc.util.macros.proxy-class :as proxy-class]
            [clojure.test :refer :all]))

(defprotocol ProtocolA
  (method1 [_ a b])
  (method2 [_ a b])
  (method3 [_ a] [_ a b]))

(defprotocol ProtocolB
  (method4 [_])
  (method5 [_ a] [_ a b]))

(deftest splice-method-lists-test
  (is (= '((a [_] a1) (a [_ _] a2b) (b [_] b1))
         (#'proxy-class/splice-method-lists
          '((a [_] a1) (a [_ _] a2) (b [_] b1))
          '((a [_ _] a2b))))))

(deftest all-interfaces-test
  (is (= [java.sql.PreparedStatement java.sql.Statement java.sql.Wrapper java.lang.AutoCloseable]
         (#'proxy-class/all-interfaces java.sql.PreparedStatement))))

(deftest proxy-class-method-test
  (let [m (some (fn [^java.lang.reflect.Method m]
                  (when (and (= (.getName m) "prepareStatement")
                             (= (count (.getParameterTypes m)) 4))
                    m))
                (.getDeclaredMethods java.sql.Connection))
        form (#'proxy-class/proxy-class-method m 'conn)]
    (is (= '(prepareStatement [_ a b c d] (.prepareStatement conn a b c d))
           form))
    (let [[method-name arglist] form]
      (is (= {:tag 'java.sql.PreparedStatement}
             (meta method-name)))
      (is (= [nil 'java.lang.String 'int 'int 'int]
             (map (comp :tag meta) arglist))))))

(deftest proxy-class-methods-test
  (is (= '((method1 [_ a b] (.method1 obj a b))
           (method2 [_ a b] (.method2 obj a b))
           (method3 [_ a b] (.method3 obj a b))
           (method3 [_ a] (.method3 obj a)))
         (#'proxy-class/proxy-class-methods bluejdbc.util.macros.proxy_class_test.ProtocolA 'obj))))

(deftest parse-body-test
  (is (= '{ProtocolA [(method1 [_ a b])
                      (method3 [_ a])
                      (method3 [_ a b])]
           ProtocolB [(method4 [_])]}
         (#'proxy-class/parse-body
          '(ProtocolA
            (method1 [_ a b])
            (method3 [_ a])
            (method3 [_ a b])

            ProtocolB
            (method4 [_]))))))

(deftest splice-methods-into-body-test
  (is (= '(ProtocolA (method1 [_ a b] "1") (method2 [_ x y] "2 override") ProtocolB (method4 [_] "4"))
         (#'proxy-class/splice-methods-into-body
          '(ProtocolA (method2 [_ x y] "2 override") ProtocolB (method4 [_] "4"))
          'ProtocolA
          '((method1 [_ a b] "1") (method2 [_ a b] "2"))))))

(deftest define-proxy-class-test
  (is (= '(potemkin.types/deftype+
              MyProxy
              [obj x y]
              ProtocolA
              (method1 [_ a b] "wow")
              (method2 [_ a b] (.method2 obj a b))
              (method3 [_ a] "cool")
              (method3 [_ a b] (.method3 obj a b))
              ProtocolB
              (method4 [_] "ok")
              (method5 [_ a] "5a")
            (method5 [_ a b] "5ab"))
         (binding [*ns* (the-ns 'bluejdbc.util.macros.proxy-class-test)]
           (macroexpand-1
            '(proxy-class/define-proxy-class MyProxy ProtocolA [obj x y]
               ProtocolA
               (method1 [_ a b] "wow")
               (method3 [_ a] "cool")

               ProtocolB
               (method4 [_] "ok")
               (method5 [_ a] "5a")
               (method5 [_ a b] "5ab")))))))

(deftest override-overloaded-method-test
  (testing "Should be able to override a single method of an overloaded arity"
    (let [[_ _ _ _ & method-forms] (macroexpand-1
                                    '(bluejdbc.util.macros.proxy-class/define-proxy-class ProxyResultSet
                                         java.sql.ResultSet [rs mta opts]
                                       java.sql.ResultSet
                                       (^Object getObject [_ ^int a ^Class b]
                                        "WOW")))]
      (is (= '([["java.lang.Object" "int"]                                (getObject [_ a] (.getObject rs a))]
               [["java.lang.Object" "java.lang.String"]                   (getObject [_ a] (.getObject rs a))]
               [["java.lang.Object" "int" "java.lang.Class"]              (getObject [_ a b] "WOW")]
               [["java.lang.Object" "int" "java.util.Map"]                (getObject [_ a b] (.getObject rs a b))]
               [["java.lang.Object" "java.lang.String" "java.lang.Class"] (getObject [_ a b] (.getObject rs a b))]
               [["java.lang.Object" "java.lang.String" "java.util.Map"]   (getObject [_ a b] (.getObject rs a b))])
             (for [[method-name :as form] method-forms
                   :when                  (= method-name 'getObject)]
               [(#'proxy-class/arglist-meta-tags (second form)) form]))))))

(deftest array-tags-test
  (testing "Make sure primitive array types are tagged correctly"
    (let [[_ _ _ _ & method-forms] (macroexpand-1
                                    '(bluejdbc.util.macros.proxy-class/define-proxy-class ProxyPreparedStatement
                                         java.sql.PreparedStatement [stmt mta opts]))
          set-bytes-method         (some #(when (= (first %) 'setBytes) %) method-forms)]
      (is (= '(setBytes [_ a b] (.setBytes stmt a b))
             set-bytes-method))
      (let [[_ [_ _ b]] set-bytes-method]
        (is (= 'bytes
               (:tag (meta b))))))))
